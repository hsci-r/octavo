package services

import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.document.IntPoint
import scala.util.Try
import org.apache.lucene.search.Query
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.core.KeywordAnalyzer
import javax.inject.Inject
import javax.inject.Singleton
import play.api.Configuration
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.similarities.SimilarityBase
import org.apache.lucene.search.similarities.BasicStats
import org.apache.lucene.search.Explanation
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.index.DirectoryReader
import java.nio.file.FileSystems
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.Terms
import org.apache.lucene.util.BytesRef
import org.apache.lucene.index.TermsEnum
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.DocValues
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.SortedDocValues
import org.apache.lucene.search.SimpleCollector
import scala.collection.mutable.HashSet
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import parameters.SumScaling
import org.apache.lucene.search.TimeLimitingCollector
import scala.collection.mutable.HashMap
import scala.util.matching.Regex.Match
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.AutomatonQuery
import org.apache.lucene.util.automaton.Automata
import org.apache.lucene.index.Term
import org.apache.lucene.util.automaton.Automaton
import org.apache.lucene.search.highlight.QueryTermExtractor
import org.apache.lucene.search.TotalHitCountCollector
import play.api.Logger
import java.io.File
import java.io.FileInputStream
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import enumeratum.EnumEntry
import enumeratum.Enum
import org.apache.lucene.index.LeafReader
import java.util.Collections

object IndexAccess {
  
  private val numWorkers = sys.runtime.availableProcessors / 2
  private val queueCapacity = 10
  
  val longTaskExecutionContext = ExecutionContext.fromExecutorService(
   new ThreadPoolExecutor(
     numWorkers, numWorkers,
     0L, TimeUnit.SECONDS,
     new ArrayBlockingQueue[Runnable](queueCapacity) {
       override def offer(e: Runnable) = {
         put(e)
         true
       }
     }
   )
  )
  
  val shortTaskExecutionContext = ExecutionContext.Implicits.global
  
  BooleanQuery.setMaxClauseCount(Int.MaxValue)

  private val standardAnalyzer = new StandardAnalyzer()
  
  private val termFrequencySimilarity = new SimilarityBase() {
    override def score(stats: BasicStats, freq: Float, docLen: Float): Float = {
      return freq
    }
    override def explain(stats: BasicStats, doc: Int, freq: Explanation, docLen: Float): Explanation = {
      return Explanation.`match`(freq.getValue,"")
    }
    override def toString(): String = {
      ""
    }
  }
  
  private case class TermsEnumToBytesRefIterator(te: TermsEnum) extends Iterator[BytesRef] {
    var br = te.next()
    def next(): BytesRef = {
      val ret = br
      br = te.next()
      return ret
    }
    def hasNext() = br != null
  }
  
  private case class TermsToBytesRefIterable(te: Terms) extends Iterable[BytesRef] {
    def iterator(): Iterator[BytesRef] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefIterator(te: TermsEnum): Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)

  private case class TermsEnumToBytesRefAndDocFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Int)] {
    var br = te.next()
    def next(): (BytesRef,Int) = {
      val ret = (br, te.docFreq)
      br = te.next()
      return ret
    }
    def hasNext() = br != null
  }
  
  private case class TermsToBytesRefAndDocFreqIterable(te: Terms) extends Iterable[(BytesRef,Int)] {
    def iterator(): Iterator[(BytesRef,Int)] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefAndDocFreqIterator(te: TermsEnum): Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)

  private case class TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Int,Long)] {
    var br = te.next()
    def next(): (BytesRef,Int,Long) = {
      val ret = (br, te.docFreq, te.totalTermFreq)
      br = te.next()
      return ret
    }
    def hasNext() = br != null
  }

  private case class TermsToBytesRefAndDocFreqAndTotalTermFreqIterable(te: Terms) extends Iterable[(BytesRef,Int,Long)] {
    def iterator(): Iterator[(BytesRef,Int,Long)] = te.iterator
  }

  private implicit def termsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te: TermsEnum): Iterator[(BytesRef,Int,Long)] = TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te)

  case class RichTermsEnum(te: TermsEnum) {
    def asBytesRefIterator(): Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)
    def asBytesRefAndDocFreqIterator(): Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)
  }
  
  implicit def termsEnumToRichTermsEnum(te: TermsEnum) = RichTermsEnum(te)
  
  case class RichTerms(te: Terms) {
    def asBytesRefIterable(): Iterable[BytesRef] = TermsToBytesRefIterable(te)
    def asBytesRefAndDocFreqIterable(): Iterable[(BytesRef,Int)] = TermsToBytesRefAndDocFreqIterable(te)
    def asBytesRefAndDocFreqAndTotalTermFreqIterable(): Iterable[(BytesRef,Int,Long)] = TermsToBytesRefAndDocFreqAndTotalTermFreqIterable(te)
  }
  
  implicit def termsToRichTerms(te: Terms) = RichTerms(te)
  
  def getHitCountForQuery(is: IndexSearcher, q: Query): Long = {
    val hc = new TotalHitCountCollector()
    is.search(q,hc)
    return hc.getTotalHits
  }

  
  def docFreq(ir: IndexReader, term: Long): Int = {
    val it = ir.leaves.get(0).reader.terms("content").iterator
    it.seekExact(term)
    return it.docFreq
  }

  def totalTermFreq(ir: IndexReader, term: Long): Long = {
    val it = ir.leaves.get(0).reader.terms("content").iterator
    it.seekExact(term)
    return it.totalTermFreq
  }

  def termOrdToTerm(ir: IndexReader, term: Long): String = {
    val it = ir.leaves.get(0).reader.terms("content").iterator
    it.seekExact(term)
    return it.term.utf8ToString
  }
  
  private def getMatchingValuesFromSortedDocValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Iterable[BytesRef] = {
    val ret = new HashSet[BytesRef]
    tlc.get.setCollector(new SimpleCollector() {
      
      override def needsScores: Boolean = false
      
      var dv: SortedDocValues = null

      override def collect(doc: Int) {
        ret += BytesRef.deepCopyOf(this.dv.get(doc))
      }
      
      override def doSetNextReader(context: LeafReaderContext) = {
        this.dv = DocValues.getSorted(context.reader, field)
      }
    })
    is.search(q, tlc.get)
    Logger.debug(f"SortedDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
    ret
  }

  private def getMatchingValuesFromNumericDocValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Iterable[BytesRef] = {
    val ret = new HashSet[BytesRef]
    tlc.get.setCollector(new SimpleCollector() {
      
      override def needsScores: Boolean = false
      
      var dv: NumericDocValues = null

      override def collect(doc: Int) {
        ret += new BytesRef(""+this.dv.get(doc))
      }
      
      override def doSetNextReader(context: LeafReaderContext) = {
        this.dv = DocValues.getNumeric(context.reader, field)
      }
    })
    is.search(q, tlc.get)
    Logger.debug(f"NumericDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
    ret
  }
  
  def extractContentTermsFromQuery(q: Query): Seq[String] = QueryTermExtractor.getTerms(q, false, "content").map(_.getTerm).toSeq

}

@Singleton
class IndexAccess @Inject() (config: Configuration) {
  
  import IndexAccess._
 
  val path = config.getString("index.path").getOrElse("/srv/ecco")
  
  private val readers: collection.mutable.Map[String,IndexReader] = new HashMap[String,IndexReader]  
  private val tfSearchers: collection.mutable.Map[String,IndexSearcher] = new HashMap[String,IndexSearcher]
  private val tfidfSearchers: collection.mutable.Map[String,IndexSearcher] = new HashMap[String,IndexSearcher]
  
  sealed abstract class QueryByType extends EnumEntry {
    def apply(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Iterable[BytesRef] 
  }  
  
  object QueryByType extends Enum[QueryByType] {
    case object NUMERIC extends QueryByType {
      def apply(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Iterable[BytesRef] = getMatchingValuesFromNumericDocValues(is, q, field)
    }
    case object SORTED extends QueryByType {
      def apply(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Iterable[BytesRef] = getMatchingValuesFromSortedDocValues(is, q, field)
    }
    val values = findValues
  }
  
  case class LevelMetadata(
    id: String,
    term: String,
    index: String) {
    val termAsTerm = new Term(term,"")
  }

  case class IndexMetadata(
    levels: Seq[LevelMetadata],
    intPointFields: Set[String],
    termVectorFields: Set[String],
    sortedDocValuesFields: Set[String],
    storedSingularFields: Set[String],
    storedMultiFields: Set[String],
    numericDocValuesFields: Set[String]
  ) {
    val levelOrder: Map[String,Int] = levels.map(_.id).zipWithIndex.toMap
    val levelMap: Map[String,LevelMetadata] = levels.map(l => (l.id,l)).toMap
    val levelType: Map[String,QueryByType] = levels.map(l => (l.id,if (numericDocValuesFields.contains(l.term)) QueryByType.NUMERIC else QueryByType.SORTED)).toMap
    def getter(lr: LeafReader, field: String): (Int) => Iterable[String] = {
      if (storedSingularFields.contains(field) || storedMultiFields.contains(field)) {
        val fieldS = Collections.singleton(field)
        return (doc: Int) => lr.document(doc,fieldS).getValues(field).toSeq
      }
      if (sortedDocValuesFields.contains(field)) {
        val dvs = DocValues.getSorted(lr, field)
        return (doc: Int) => Seq(dvs.get(doc).utf8ToString())
      }
      if (numericDocValuesFields.contains(field)) {
        val dvs = DocValues.getNumeric(lr, field)
        return (doc: Int) => Seq(""+dvs.get(doc))
      }
      if (termVectorFields.contains(field))
        return (doc: Int) => lr.getTermVector(doc, field).asBytesRefIterable().map(_.utf8ToString)
      return null
    }
  }
  
  def readLevelMetadata(c: JsValue) = LevelMetadata(
      (c \ "id").as[String],
      (c \ "term").as[String],
      (c \ "index").as[String]
  )
  
  def readIndexMetadata(c: JsValue) = IndexMetadata(
    (c \ "levels").as[Seq[JsValue]].map(readLevelMetadata(_)),
    (c \ "intPointFields").as[Set[String]],
    (c \ "termVectorFields").as[Set[String]],
    (c \ "sortedDocValuesFields").as[Set[String]],
    (c \ "storedSingularFields").as[Set[String]],
    (c \ "storedMultiFields").as[Set[String]],
    (c \ "numericDocValuesFields").as[Set[String]]
  )
  
  val indexMetadata: IndexMetadata = readIndexMetadata(Json.parse(new FileInputStream(new File(path+"/indexmeta.json"))))
  
  for (level <- indexMetadata.levels) {
    readers.put(level.id, DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath(path+"/"+level.index))))
    tfSearchers.put(level.id, {
      val is = new IndexSearcher(readers(level.id))
      is.setSimilarity(termFrequencySimilarity)
      is
    })
    tfidfSearchers.put(level.id, new IndexSearcher(readers(level.id)))
  }

  def reader(level: String): IndexReader = readers(level)
  
  def searcher(level: String, sumScaling: SumScaling): IndexSearcher = {
    sumScaling match {
      case SumScaling.DF =>
        tfidfSearchers(level)
      case _ =>
        tfSearchers(level)
    }
  }
  
  val analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),
      (indexMetadata.storedMultiFields ++ indexMetadata.storedSingularFields).map((_,standardAnalyzer)).toMap[String,Analyzer].asJava)
  
  val queryParsers = new ThreadLocal[QueryParser] {
    
    override def initialValue(): QueryParser = new QueryParser("content",analyzer) {
      override def getRangeQuery(field: String, part1: String, part2: String, startInclusive: Boolean, endInclusive: Boolean): Query = {
        if (indexMetadata.intPointFields.contains(field)) {
          val low = Try(if (startInclusive) part1.toInt else part1.toInt + 1).getOrElse(Int.MinValue)
          val high = Try(if (endInclusive) part2.toInt else part2.toInt - 1).getOrElse(Int.MaxValue)
          IntPoint.newRangeQuery(field, low, high)
        } else super.getRangeQuery(field,part1,part2,startInclusive,endInclusive) 
      } 
    }

  }
   
  private val queryPartStart = "(?<!\\\\)<".r
  private val queryPartEnd = "(?<!\\\\)>".r
  
  private val documentIDTerm = new Term("documentID","")
  private val documentPartIDTerm = new Term("partID","")
  private val sectionIDTerm = new Term("sectionID","")
  private val paragraphIDTerm = new Term("paragraphID","")

  
  private def runSubQuery(queryLevel: String, query: Query, targetLevel: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Query = {
    val (idTerm: Term, values: Iterable[BytesRef]) = 
      if (indexMetadata.levelOrder(queryLevel)<indexMetadata.levelOrder(targetLevel)) 
        (indexMetadata.levelMap(targetLevel).termAsTerm, indexMetadata.levelType(queryLevel)(searcher(queryLevel, SumScaling.ABSOLUTE), query, indexMetadata.levelMap(targetLevel).term)) 
      else 
        (indexMetadata.levelMap(queryLevel).termAsTerm, indexMetadata.levelType(targetLevel)(searcher(targetLevel, SumScaling.ABSOLUTE), query, indexMetadata.levelMap(queryLevel).term))
    new AutomatonQuery(idTerm,Automata.makeStringUnion(values.asJavaCollection))
  }
  
  private def processQueryInternal(queryIn: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (String,Query,String) = {
    val queryLevel = queryIn.substring(1,queryIn.indexOf('|')).toUpperCase
    val targetLevel = queryIn.substring(queryIn.lastIndexOf('|') + 1, queryIn.length - 1).toUpperCase
    var query = queryIn.substring(queryIn.indexOf('|') + 1, queryIn.lastIndexOf('|'))
    val replacements = new HashMap[String,Query]
    var firstStart = queryPartStart.findFirstMatchIn(query)
    while (firstStart.isDefined) { // we have (more) subqueries
      val ends = queryPartEnd.findAllMatchIn(query)
      var curEnd = ends.next().start
      var neededEnds = queryPartStart.findAllIn(query.substring(0, curEnd)).size - 1
      while (neededEnds > 0) curEnd = ends.next().start
      val (subQueryQueryLevel, subQuery, subQueryTargetLevel) = processQueryInternal(query.substring(firstStart.get.start, curEnd + 1))
      val processedSubQuery = if (subQueryQueryLevel == queryLevel && subQueryTargetLevel == targetLevel) subQuery else runSubQuery(subQueryQueryLevel,subQuery,subQueryTargetLevel)
      replacements += (("" + (replacements.size + 1)) -> processedSubQuery)
      query = query.substring(0, firstStart.get.start) + "MAGIC:" + replacements.size + query.substring(curEnd + 1)
      firstStart = queryPartStart.findFirstMatchIn(query)
    }
    Logger.debug(s"Query ${queryIn} rewritten to $query with replacements $replacements.")
    val q = queryParsers.get.parse(query)
    if (replacements.isEmpty) (queryLevel,q,targetLevel) 
    else {
      val bqb = new BooleanQuery.Builder()
      for (clause <- q.asInstanceOf[BooleanQuery].clauses.asScala)
        if (clause.getQuery.isInstanceOf[TermQuery]) {
          val tq = clause.getQuery.asInstanceOf[TermQuery]
          if (tq.getTerm.field == "MAGIC") bqb.add(replacements(tq.getTerm.text),clause.getOccur)
          else bqb.add(clause)
        } else bqb.add(clause)
      (queryLevel,bqb.build,targetLevel)
    }
  }
  
  def buildFinalQueryRunningSubQueries(query: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (String, Query) = {
    val (queryLevel, q, targetLevel) = processQueryInternal(query)
    if (queryLevel==targetLevel) (targetLevel,q)
    else (targetLevel,runSubQuery(queryLevel,q,targetLevel))
  }
  
}
