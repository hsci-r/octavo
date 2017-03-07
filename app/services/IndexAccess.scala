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
import parameters.Level
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

@Singleton
class IndexAccess @Inject() (config: Configuration) {
  
  import IndexAccess._
 
  private val path = config.getString("index.path").getOrElse("/srv/ecco")

  private val documentReader = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath(path+"/dindex")))
  private val documentTermFrequencySearcher = {
    val is = new IndexSearcher(documentReader)
    is.setSimilarity(termFrequencySimilarity)
    is
  }
  private val documentTFIDFSearcher = new IndexSearcher(documentReader)
  
  private val documentPartReader = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath(path+"/dpindex")))
  private val documentPartTermFrequencySearcher = {
    val is = new IndexSearcher(documentPartReader)
    is.setSimilarity(termFrequencySimilarity)
    is
  }
  private val documentPartTFIDFSearcher = new IndexSearcher(documentPartReader)
  
  private val sectionReader = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath(path+"/sindex")))
  private val sectionTermFrequencySearcher = {
    val is = new IndexSearcher(sectionReader)
    is.setSimilarity(termFrequencySimilarity)
    is
  }
  private val sectionTFIDFSearcher = new IndexSearcher(sectionReader)
  
  private val paragraphReader = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath(path+"/pindex")))
  private val paragraphTermFrequencySearcher = {
    val is = new IndexSearcher(paragraphReader)
    is.setSimilarity(termFrequencySimilarity)
    is
  }
  private val paragraphTFIDFSearcher = new IndexSearcher(paragraphReader)
  
  def reader(level: Level.Value): IndexReader = level match {
    case Level.DOCUMENT => documentReader
    case Level.DOCUMENTPART => documentPartReader
    case Level.SECTION => sectionReader
    case Level.PARAGRAPH => paragraphReader
  }
  
  def searcher(level: Level.Value, sumScaling: SumScaling): IndexSearcher = {
    sumScaling match {
      case SumScaling.DF => 
        level match {
          case Level.DOCUMENT => documentTFIDFSearcher
          case Level.DOCUMENTPART => documentPartTFIDFSearcher
          case Level.SECTION => sectionTFIDFSearcher
          case Level.PARAGRAPH => paragraphTFIDFSearcher
        }
      case _ =>
        level match {
          case Level.DOCUMENT => documentTermFrequencySearcher
          case Level.DOCUMENTPART => documentPartTermFrequencySearcher
          case Level.SECTION => sectionTermFrequencySearcher
          case Level.PARAGRAPH => paragraphTermFrequencySearcher
        }
    }
  }
  
  private val queryPartStart = "(?<!\\\\)<".r
  private val queryPartEnd = "(?<!\\\\)>".r
  
  private val documentIDTerm = new Term("documentID","")
  private val documentPartIDTerm = new Term("partID","")
  private val sectionIDTerm = new Term("sectionID","")
  private val paragraphIDTerm = new Term("paragraphID","")

  
  private def runSubQuery(queryLevel: Level.Value, query: Query, targetLevel: Level.Value)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Query = {
    var idTerm: Term = null
    var values: Iterable[BytesRef] = null
    targetLevel match {
      case Level.PARAGRAPH =>
        values = queryLevel match {
          case Level.DOCUMENT =>
            idTerm = documentIDTerm
            getMatchingValuesFromSortedDocValues(searcher(targetLevel, SumScaling.ABSOLUTE), query, "documentID")
          case Level.DOCUMENTPART =>
            idTerm = documentPartIDTerm
            getMatchingValuesFromNumericDocValues(searcher(targetLevel, SumScaling.ABSOLUTE), query, "partID")
          case Level.SECTION =>
            idTerm = sectionIDTerm
            getMatchingValuesFromNumericDocValues(searcher(targetLevel, SumScaling.ABSOLUTE), query, "sectionID")
          case Level.PARAGRAPH => 
            idTerm = paragraphIDTerm
            getMatchingValuesFromNumericDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "paragraphID")
        }
      case Level.SECTION => 
        values = queryLevel match {
          case Level.DOCUMENT =>
            idTerm = documentIDTerm
            getMatchingValuesFromSortedDocValues(searcher(targetLevel, SumScaling.ABSOLUTE), query, "documentID")
          case Level.DOCUMENTPART =>
            idTerm = documentPartIDTerm
            getMatchingValuesFromNumericDocValues(searcher(targetLevel, SumScaling.ABSOLUTE), query, "partID")
          case Level.SECTION =>
            idTerm = sectionIDTerm
            getMatchingValuesFromNumericDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "sectionID")
          case Level.PARAGRAPH =>
            idTerm = sectionIDTerm
            getMatchingValuesFromNumericDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "sectionID")
        }
      case Level.DOCUMENTPART =>
        values = queryLevel match {
          case Level.DOCUMENT =>
            idTerm = documentIDTerm
            getMatchingValuesFromSortedDocValues(searcher(targetLevel, SumScaling.ABSOLUTE), query, "documentID")
          case Level.DOCUMENTPART => 
            idTerm = documentPartIDTerm
            getMatchingValuesFromNumericDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "partID")
          case Level.SECTION => 
            idTerm = documentPartIDTerm
            getMatchingValuesFromNumericDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "partID")
          case Level.PARAGRAPH => 
            idTerm = documentPartIDTerm
            getMatchingValuesFromNumericDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "partID")
        }
      case Level.DOCUMENT => 
        idTerm = documentIDTerm
        values = queryLevel match {
          case Level.DOCUMENT => getMatchingValuesFromSortedDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "documentID")
          case Level.DOCUMENTPART => getMatchingValuesFromSortedDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "documentID")
          case Level.SECTION => getMatchingValuesFromSortedDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "documentID")
          case Level.PARAGRAPH => getMatchingValuesFromSortedDocValues(searcher(queryLevel, SumScaling.ABSOLUTE), query, "documentID")
        }
    }
    new AutomatonQuery(idTerm,Automata.makeStringUnion(values.asJavaCollection))
  }
  
  private def processQueryInternal(queryIn: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (Level.Value,Query,Level.Value) = {
    val queryLevel = Level.withName(queryIn.substring(1,queryIn.indexOf('|')).toUpperCase)
    val targetLevel = Level.withName(queryIn.substring(queryIn.lastIndexOf('|') + 1, queryIn.length - 1).toUpperCase)
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
  
  def buildFinalQueryRunningSubQueries(query: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (Level.Value, Query) = {
    val (queryLevel, q, targetLevel) = processQueryInternal(query)
    if (queryLevel==targetLevel) (targetLevel,q)
    else (targetLevel,runSubQuery(queryLevel,q,targetLevel))
  }
  
}

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
  
  val analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),Map[String,Analyzer]("content"->standardAnalyzer,"fullTitle"->standardAnalyzer).asJava)
  
  val queryParsers = new ThreadLocal[QueryParser] {
    
    override def initialValue(): QueryParser = new QueryParser("content",analyzer) {
      override def getRangeQuery(field: String, part1: String, part2: String, startInclusive: Boolean, endInclusive: Boolean): Query = {
        field match {
          case "pubDateStart" | "pubDateEnd" | "contentTokens" | "length" | "totalPages" | "documentLength" =>
            val low = Try(if (startInclusive) part1.toInt else part1.toInt + 1).getOrElse(Int.MinValue)
            val high = Try(if (endInclusive) part2.toInt else part2.toInt - 1).getOrElse(Int.MaxValue)
            IntPoint.newRangeQuery(field, low, high)
          case _ => super.getRangeQuery(field,part1,part2,startInclusive,endInclusive) 
        }
      } 
    }

  }
   
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

  case class RichTermsEnum(te: TermsEnum) {
    def asBytesRefIterator(): Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)
    def asBytesRefAndDocFreqIterator(): Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)
  }
  
  implicit def termsEnumToRichTermsEnum(te: TermsEnum) = RichTermsEnum(te)
  
  case class RichTerms(te: Terms) {
    def asBytesRefIterable(): Iterable[BytesRef] = TermsToBytesRefIterable(te)
    def asBytesRefAndDocFreqIterable(): Iterable[(BytesRef,Int)] = TermsToBytesRefAndDocFreqIterable(te)
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
