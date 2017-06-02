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
import org.apache.lucene.analysis.CharArraySet
import scala.language.implicitConversions
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import fi.seco.lucene.MorphologicalAnalyzer
import java.util.Locale
import org.apache.lucene.search.MultiTermQuery
import org.apache.lucene.util.AttributeSource
import org.apache.lucene.index.TermsEnum.SeekStatus
import org.apache.lucene.index.PostingsEnum
import org.apache.lucene.index.FilteredTermsEnum
import org.apache.lucene.index.FilteredTermsEnum.AcceptStatus
import scala.collection.Searching._
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.Directory
import java.nio.file.Path
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.store.IOContext
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.forkjoin.ForkJoinPool.ForkJoinWorkerThreadFactory
import scala.concurrent.forkjoin.ForkJoinWorkerThread
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.apache.lucene.search.TermInSetQuery
import java.util.SortedSet
import org.apache.lucene.document.LongPoint
import org.joda.time.format.ISODateTimeFormat
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.queryparser.classic.QueryParser.Operator
import scala.collection.parallel.ForkJoinTaskSupport

object IndexAccess {
    
  private val numShortWorkers = sys.runtime.availableProcessors
  private val numLongWorkers = Math.max(sys.runtime.availableProcessors - 2, 1)
  private val queueCapacity = 10
  
  val longTaskForkJoinPool = new ForkJoinPool(numLongWorkers, new ForkJoinWorkerThreadFactory() {
     override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
       val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
       worker.setName("long-task-worker-" + worker.getPoolIndex())
       worker
     }
   }, null, true)
  val longTaskTaskSupport = new ForkJoinTaskSupport(longTaskForkJoinPool)
  val longTaskExecutionContext = ExecutionContext.fromExecutorService(longTaskForkJoinPool)
  
  val shortTaskForkJoinPool = new ForkJoinPool(numShortWorkers, new ForkJoinWorkerThreadFactory() {
     override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
       val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
       worker.setName("short-task-worker-" + worker.getPoolIndex())
       worker
     }
   }, null, true)
  val shortTaskTaskSupport = new ForkJoinTaskSupport(shortTaskForkJoinPool)
  val shortTaskExecutionContext = ExecutionContext.fromExecutorService(shortTaskForkJoinPool)

  BooleanQuery.setMaxClauseCount(Int.MaxValue)

  //private val standardAnayzer = new StandardAnalyzer(CharArraySet.EMPTY_SET)
  
  private val whitespaceAnalyzer = new WhitespaceAnalyzer()
  
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
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    def next(): BytesRef = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      return br
    }
    def hasNext() = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }
  
  private case class TermsToBytesRefIterable(te: Terms) extends Iterable[BytesRef] {
    def iterator(): Iterator[BytesRef] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefIterator(te: TermsEnum): Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)

  private case class TermsEnumToBytesRefAndDocFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Int)] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    def next(): (BytesRef,Int) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.docFreq)
      return ret
    }
    def hasNext() = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }
  
  private case class TermsToBytesRefAndDocFreqIterable(te: Terms) extends Iterable[(BytesRef,Int)] {
    def iterator(): Iterator[(BytesRef,Int)] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefAndDocFreqIterator(te: TermsEnum): Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)

  private case class TermsEnumToBytesRefAndTotalTermFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Long)] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    def next(): (BytesRef,Long) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.totalTermFreq)
      return ret
    }
    def hasNext() = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }
  
  private case class TermsToBytesRefAndTotalTermFreqIterable(te: Terms) extends Iterable[(BytesRef,Long)] {
    def iterator(): Iterator[(BytesRef,Long)] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefAndTotalTermFreqIterator(te: TermsEnum): Iterator[(BytesRef,Long)] = TermsEnumToBytesRefAndTotalTermFreqIterator(te)

  
  private case class TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Int,Long)] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    def next(): (BytesRef,Int,Long) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.docFreq, te.totalTermFreq)
      return ret
    }
    def hasNext() = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }

  private case class TermsToBytesRefAndDocFreqAndTotalTermFreqIterable(te: Terms) extends Iterable[(BytesRef,Int,Long)] {
    def iterator(): Iterator[(BytesRef,Int,Long)] = te.iterator
  }

  private implicit def termsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te: TermsEnum): Iterator[(BytesRef,Int,Long)] = TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te)

  case class RichTermsEnum(te: TermsEnum) {
    def asBytesRefIterator(): Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)
    def asBytesRefAndDocFreqIterator(): Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)
    def asBytesRefAndDocFreqAndTotalTermFreqIterator(): Iterator[(BytesRef,Int,Long)] = TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te)
    def asBytesRefAndTotalTermFreqIterator(): Iterator[(BytesRef,Long)] = TermsEnumToBytesRefAndTotalTermFreqIterator(te)
  }
  
  implicit def termsEnumToRichTermsEnum(te: TermsEnum) = RichTermsEnum(te)
  
  case class RichTerms(te: Terms) {
    def asBytesRefIterable(): Iterable[BytesRef] = TermsToBytesRefIterable(te)
    def asBytesRefAndDocFreqIterable(): Iterable[(BytesRef,Int)] = TermsToBytesRefAndDocFreqIterable(te)
    def asBytesRefAndTotalTermFreqIterable(): Iterable[(BytesRef,Long)] = TermsToBytesRefAndTotalTermFreqIterable(te)
    def asBytesRefAndDocFreqAndTotalTermFreqIterable(): Iterable[(BytesRef,Int,Long)] = TermsToBytesRefAndDocFreqAndTotalTermFreqIterable(te)
  }
  
  implicit def termsToRichTerms(te: Terms) = RichTerms(te)
  
  def getHitCountForQuery(is: IndexSearcher, q: Query): Long = {
    val hc = new TotalHitCountCollector()
    is.search(q,hc)
    return hc.getTotalHits
  }

  
  def getMatchingValuesFromSortedDocValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): scala.collection.Set[BytesRef] = {
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

  def getMatchingValuesFromNumericDocValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): scala.collection.Set[BytesRef] = {
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
  
}

@Singleton
class IndexAccessProvider @Inject() (config: Configuration) {
  private val defaultIndex = config.getString("index.path").map(new IndexAccess(_))
  private val indexAccesses = {
    val m = config.getConfig("indices")
      .map(c => c.keys.map(k => (k, new IndexAccess(c.getString(k).get))).toMap).getOrElse(Map.empty)
    if (defaultIndex.isDefined) m.withDefaultValue(defaultIndex.get)
    else m
  }
  def apply(id: String): IndexAccess = indexAccesses(id)
}

sealed abstract class QueryByType extends EnumEntry {
  def apply(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Iterable[BytesRef] 
}  
  
object QueryByType extends Enum[QueryByType] {
  import IndexAccess._
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
  index: String,
  preload: Boolean) {
  val termAsTerm = new Term(term,"")
  def toJson = Json.obj("id"->id,"term"->term,"index"->index,"preload"->preload)
}

case class IndexMetadata(
  indexName: String,
  indexVersion: String,
  indexType: String,
  contentField: String,
  contentTokensField: String,
  levels: Seq[LevelMetadata],
  defaultLevelS: Option[String],
  indexingAnalyzersAsText: Map[String,String],
  textFields: Set[String],
  intPointFields: Set[String],
  longPointFields: Set[String],
  termVectorFields: Set[String],
  sortedDocValuesFields: Set[String],
  storedSingularFields: Set[String],
  storedMultiFields: Set[String],
  numericDocValuesFields: Set[String],
  jsonFields: Set[String]
) {
  import IndexAccess._
  
  val directoryCreator: (Path) => Directory = (path: Path) => indexType match {
    case "MMapDirectory" =>
      new MMapDirectory(path)        
    case "RAMDirectory" =>
      val id = new NIOFSDirectory(path)
      val d = new RAMDirectory(id, new IOContext())
      id.close()
      d
    case "SimpleFSDirectory" => new SimpleFSDirectory(path)
    case "NIOFSDirectory" => new NIOFSDirectory(path)
    case any => throw new IllegalArgumentException("Unknown directory type "+any)
  }
  val indexingAnalyzers: Map[String,Analyzer] = indexingAnalyzersAsText.mapValues(_ match {
    case "StandardAnalyzer" => new StandardAnalyzer(CharArraySet.EMPTY_SET)
    case a if a.startsWith("MorphologicalAnalyzer_") => new MorphologicalAnalyzer(new Locale(a.substring(23)))  
    case any => throw new IllegalArgumentException("Unknown analyzer type "+any) 
  }).withDefaultValue(new KeywordAnalyzer()) 
  val levelOrder: Map[String,Int] = levels.map(_.id).zipWithIndex.toMap
  val levelMap: Map[String,LevelMetadata] = levels.map(l => (l.id,l)).toMap
  val defaultLevel: LevelMetadata = defaultLevelS.map(levelMap(_)).getOrElse(levels.last)
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
    def toJson = Json.obj(
        "name"->indexName,
        "version"->indexVersion,
        "indexType"->indexType,
        "contentField"->contentField,
        "contentTokensField"->contentTokensField,
        "levels"->levels.map(_.toJson),
        "defaultLevel"->defaultLevel.id,
        "indexingAnalyzers"->indexingAnalyzersAsText,
        "textFields"->textFields,
        "intPointFields"->intPointFields,
        "longPointFields"->longPointFields,
        "termVectorFields"->termVectorFields,
        "sortedDocValuesFields"->sortedDocValuesFields,
        "storedSingularFields"->storedSingularFields,
        "storedMultiFields"->storedMultiFields,
        "numericDocValuesFields"->numericDocValuesFields,
        "jsonFields"->jsonFields)

}

class IndexAccess(path: String) {
  
  import IndexAccess._
 
  private val readers: collection.mutable.Map[String,IndexReader] = new HashMap[String,IndexReader]  
  private val tfSearchers: collection.mutable.Map[String,IndexSearcher] = new HashMap[String,IndexSearcher]
  private val tfidfSearchers: collection.mutable.Map[String,IndexSearcher] = new HashMap[String,IndexSearcher]
  
  def readLevelMetadata(c: JsValue) = LevelMetadata(
      (c \ "id").as[String],
      (c \ "term").as[String],
      (c \ "index").as[String],
      (c \ "preload").asOpt[Boolean].getOrElse(false)
  )
  
  def readIndexMetadata(c: JsValue) = IndexMetadata(
    (c \ "name").as[String],
    (c \ "version").as[String],
    (c \ "indexType").asOpt[String].getOrElse("MMapDirectory"),
    (c \ "contentField").as[String],
    (c \ "contentTokensField").as[String],
    (c \ "levels").as[Seq[JsValue]].map(readLevelMetadata(_)),
    (c \ "defaultLevel").asOpt[String],
    (c \ "indexingAnalyzers").asOpt[Map[String,String]].getOrElse(Map.empty),
    (c \ "textFields").as[Set[String]],
    (c \ "intPointFields").as[Set[String]],
    (c \ "longPointFields").asOpt[Set[String]].getOrElse(Set.empty),
    (c \ "termVectorFields").as[Set[String]],
    (c \ "sortedDocValuesFields").as[Set[String]],
    (c \ "storedSingularFields").as[Set[String]],
    (c \ "storedMultiFields").as[Set[String]],
    (c \ "numericDocValuesFields").as[Set[String]],
    (c \ "jsonFields").asOpt[Set[String]].getOrElse(Set.empty)
  )
  
  val indexMetadata: IndexMetadata = readIndexMetadata(Json.parse(new FileInputStream(new File(path+"/indexmeta.json"))))
  
  {
    val readerFs = for (level <- indexMetadata.levels) yield Future {
      Logger.info("Initializing index at "+path+"/"+level.index)
      val directory = indexMetadata.directoryCreator(FileSystems.getDefault().getPath(path+"/"+level.index))
      if (level.preload && directory.isInstanceOf[MMapDirectory]) directory.asInstanceOf[MMapDirectory].setPreload(true)
      val reader = DirectoryReader.open(directory)
      Logger.info("Initialized index at "+path+"/"+level.index)
      (level.id, reader)
    }(longTaskExecutionContext)
    for (readerF <- readerFs) {
      val (id, reader) = Await.result(readerF, Duration.Inf)
      readers.put(id, reader)
      val tfSearcher = new IndexSearcher(reader)
      tfSearcher.setSimilarity(termFrequencySimilarity)
      tfSearchers.put(id, tfSearcher)
      tfidfSearchers.put(id, new IndexSearcher(reader))
    }
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
  
  val queryAnalyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),
      (indexMetadata.textFields).map((_,whitespaceAnalyzer)).toMap[String,Analyzer].asJava)
  
  val queryParsers = new ThreadLocal[QueryParser] {
    
    override def initialValue(): QueryParser = {
      val qp = new QueryParser(indexMetadata.contentField,queryAnalyzer) {
        override def getRangeQuery(field: String, part1: String, part2: String, startInclusive: Boolean, endInclusive: Boolean): Query = {
          if (indexMetadata.intPointFields.contains(field)) {
            val low = if (part1.equals("*")) Int.MinValue else if (startInclusive) part1.toInt else part1.toInt + 1
            val high = if (part2.equals("*")) Int.MaxValue else if (endInclusive) part2.toInt else part2.toInt - 1
            IntPoint.newRangeQuery(field, low, high)
          } else if (indexMetadata.longPointFields.contains(field)) {
             val low = if (part1.equals("*")) Long.MinValue else {
              val low = if (part1.contains("-")) {
                ISODateTimeFormat.dateOptionalTimeParser().parseMillis(part1)
              } else part1.toLong
              if (startInclusive) low else low + 1
            } 
            val high = if (part2.equals("*")) Long.MaxValue else {
              val high = if (part2.contains("-")) {
                ISODateTimeFormat.dateOptionalTimeParser().parseMillis(part2)
              } else part2.toLong
              if (endInclusive) high else high - 1
            } 
            LongPoint.newRangeQuery(field, low, high)
          } else super.getRangeQuery(field,part1,part2,startInclusive,endInclusive) 
        } 
      }
      qp.setAllowLeadingWildcard(true)
      qp.setLowercaseExpandedTerms(false)
      qp.setDefaultOperator(Operator.AND)
      qp.setMaxDeterminizedStates(Int.MaxValue)
      qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE)
      qp
    }

  }
   
  private val queryPartStart = "(?<!\\\\)<".r
  private val queryPartEnd = "(?<!\\\\)>".r
  
  def docFreq(ir: IndexReader, term: Long): Int = {
    val it = ir.leaves.get(0).reader.terms(indexMetadata.contentField).iterator
    it.seekExact(term)
    return it.docFreq
  }

  def totalTermFreq(ir: IndexReader, term: Long): Long = {
    val it = ir.leaves.get(0).reader.terms(indexMetadata.contentField).iterator
    it.seekExact(term)
    return it.totalTermFreq
  }

  def termOrdToTerm(ir: IndexReader, term: Long): String = {
    val it = ir.leaves.get(0).reader.terms(indexMetadata.contentField).iterator
    it.seekExact(term)
    return it.term.utf8ToString
  }
  
  def extractContentTermsFromQuery(q: Query): Seq[String] = QueryTermExtractor.getTerms(q, false, indexMetadata.contentField).map(_.getTerm).toSeq
  
  private def runSubQuery(queryLevel: String, query: Query, targetLevel: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): Future[Query] = Future {
    val (idTerm: Term, values: scala.collection.Set[BytesRef]) =
      if (!indexMetadata.levelOrder.contains(targetLevel)) 
        (new Term(targetLevel,""), (if (indexMetadata.numericDocValuesFields.contains(targetLevel)) QueryByType.NUMERIC else QueryByType.SORTED)(searcher(queryLevel, SumScaling.ABSOLUTE), query, targetLevel)) 
      else if (indexMetadata.levelOrder(queryLevel)<indexMetadata.levelOrder(targetLevel)) // DOCUMENT < PARAGRAPH
        (indexMetadata.levelMap(queryLevel).termAsTerm, indexMetadata.levelType(queryLevel)(searcher(targetLevel, SumScaling.ABSOLUTE), query, indexMetadata.levelMap(queryLevel).term))
      else 
        (indexMetadata.levelMap(targetLevel).termAsTerm, indexMetadata.levelType(targetLevel)(searcher(queryLevel, SumScaling.ABSOLUTE), query, indexMetadata.levelMap(targetLevel).term))
    new TermInSetQuery(idTerm.field, values.asJava)
  }
  
  private def processQueryInternal(queryIn: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): (String,Query,String) = {
    val queryLevel = queryIn.substring(1,queryIn.indexOf('§')).toUpperCase
    val targetLevelOrig = queryIn.substring(queryIn.lastIndexOf('§') + 1, queryIn.length - 1)
    val targetLevel = if (indexMetadata.levelOrder.contains(targetLevelOrig.toUpperCase)) targetLevelOrig.toUpperCase else targetLevelOrig
    var query = queryIn.substring(queryIn.indexOf('§') + 1, queryIn.lastIndexOf('§'))
    val replacements = new HashMap[String,Future[Query]]
    var firstStart = queryPartStart.findFirstMatchIn(query)
    while (firstStart.isDefined) { // we have (more) subqueries
      val ends = queryPartEnd.findAllMatchIn(query)
      var curEnd = ends.next().start
      var neededEnds = queryPartStart.findAllIn(query.substring(0, curEnd)).size - 1
      while (neededEnds > 0) curEnd = ends.next().start
      val (subQueryQueryLevel, subQuery, subQueryTargetLevel) = processQueryInternal(query.substring(firstStart.get.start, curEnd + 1))
      val processedSubQuery = if (subQueryQueryLevel == queryLevel && subQueryTargetLevel == targetLevel) Future.successful(subQuery) else runSubQuery(subQueryQueryLevel,subQuery,subQueryTargetLevel)
      replacements += (("" + (replacements.size + 1)) -> processedSubQuery)
      query = query.substring(0, firstStart.get.start) + "MAGIC:" + replacements.size + query.substring(curEnd + 1)
      firstStart = queryPartStart.findFirstMatchIn(query)
    }
    Logger.debug(s"Query ${queryIn} rewritten to $query with replacements $replacements.")
    val q = if (query.isEmpty) new NumericDocValuesWeightedMatchAllDocsQuery(indexMetadata.contentTokensField) else { 
      val q = queryParsers.get.parse(query) 
      if (q.isInstanceOf[BooleanQuery]) {
        val bq = q.asInstanceOf[BooleanQuery]
        if (bq.clauses.asScala.forall(c => c.getOccur == Occur.MUST_NOT || (c.getQuery.isInstanceOf[BoostQuery]) && c.getQuery.asInstanceOf[BoostQuery].getBoost == 0.0f)) {
          val bqb = new BooleanQuery.Builder()
          bqb.add(new NumericDocValuesWeightedMatchAllDocsQuery(indexMetadata.contentTokensField), Occur.MUST)
          bq.clauses.asScala.foreach(bqb.add(_))
          bqb.build
        } else bq
      } else if (q.isInstanceOf[BoostQuery] && q.asInstanceOf[BoostQuery].getBoost == 0.0) {
        val bqb = new BooleanQuery.Builder()
        bqb.add(new NumericDocValuesWeightedMatchAllDocsQuery(indexMetadata.contentTokensField), Occur.MUST)
        bqb.add(q, Occur.MUST)
        bqb.build
      } else q
    }
    if (replacements.isEmpty) (queryLevel,q,targetLevel) 
    else if (q.isInstanceOf[BooleanQuery]) {
      val bqb = new BooleanQuery.Builder()
      for (clause <- q.asInstanceOf[BooleanQuery].clauses.asScala)
        if (clause.getQuery.isInstanceOf[TermQuery]) {
          val tq = clause.getQuery.asInstanceOf[TermQuery]
          if (tq.getTerm.field == "MAGIC") bqb.add(Await.result(replacements(tq.getTerm.text), Duration.Inf),clause.getOccur)
          else bqb.add(clause)
        } else bqb.add(clause)
      (queryLevel,bqb.build,targetLevel)
    } else {
      val q2 = if (q.isInstanceOf[TermQuery]) {
          val tq = q.asInstanceOf[TermQuery]
          if (tq.getTerm.field == "MAGIC") Await.result(replacements(tq.getTerm.text), Duration.Inf)
          else q
      } else q
      (queryLevel,q2,targetLevel)
    }
  }
  
  def buildFinalQueryRunningSubQueries(query: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): (String, Query) = {
    val (queryLevel, q, targetLevel) = processQueryInternal(query)
    if (queryLevel==targetLevel) (targetLevel,q)
    else (targetLevel,Await.result(runSubQuery(queryLevel,q,targetLevel), Duration.Inf))
  }
  
}
