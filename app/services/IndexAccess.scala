package services

import java.io.{File, FileInputStream, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, Path}
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}
import java.util.regex.Pattern
import java.util.{Collections, Locale}

import com.tdunning.math.stats.{MergingDigest, TDigest}
import enumeratum.{Enum, EnumEntry}
import fi.seco.lucene.MorphologicalAnalyzer
import javax.inject.{Inject, Singleton}
import javax.script._
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env._
import jetbrains.exodus.util.LightOutputStream
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis._
import org.apache.lucene.analysis.core.{KeywordAnalyzer, WhitespaceAnalyzer, WhitespaceTokenizer}
import org.apache.lucene.analysis.miscellaneous.{HyphenatedWordsFilter, LengthFilter, PerFieldAnalyzerWrapper}
import org.apache.lucene.analysis.pattern.PatternReplaceFilter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.geo.{GeoEncodingUtils, Polygon}
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queryparser.classic.QueryParser.Operator
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search._
import org.apache.lucene.search.highlight.QueryTermExtractor
import org.apache.lucene.search.similarities.{BasicStats, SimilarityBase}
import org.apache.lucene.store._
import org.apache.lucene.util.{BytesRef, BytesRefIterator}
import org.joda.time.format.ISODateTimeFormat
import parameters.{QueryScoring, SortDirection}
import play.api.inject.ApplicationLifecycle
import play.api.libs.json.Reads._
import play.api.libs.json.{Json, _}
import play.api.{Configuration, Logger, Logging}
import services.IndexAccess.scriptEngineManager

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

object IndexAccess {

  val octavoAnalyzer = new Analyzer() {
    override def createComponents(fieldName: String) = {
      val src = new WhitespaceTokenizer()
      var tok: TokenFilter = new HyphenatedWordsFilter(src)
      tok = new PatternReplaceFilter(tok,Pattern.compile("^\\p{Punct}*(.*?)\\p{Punct}*$"),"$1", false)
      tok = new LowerCaseFilter(tok)
      tok = new LengthFilter(tok, 1, Int.MaxValue)
      new TokenStreamComponents(src,normalize(fieldName,tok))
    }

    override protected def normalize(fieldName: String, in: TokenStream): TokenStream = {
      new LowerCaseFilter(in)
    }
  }

  def docFreq(it: TermsEnum, term: Long): Int = {
    it.seekExact(term)
    it.docFreq
  }

  def totalTermFreq(it: TermsEnum, term: Long): Long = {
    it.seekExact(term)
    it.totalTermFreq
  }

  def termOrdToBytesRef(it: TermsEnum, term: Long): BytesRef = {
    it.seekExact(term)
    BytesRef.deepCopyOf(it.term)
  }

  def termOrdToString(it: TermsEnum, term: Long): String = {
    it.seekExact(term)
    it.term.utf8ToString
  }

  val scriptEngineManager = new ScriptEngineManager()

  val numShortWorkers = Math.max(sys.runtime.availableProcessors,2)
  val numLongWorkers = Math.max(sys.runtime.availableProcessors - 2, 1)

  private def createBlockingSupportingForkJoinWorkerThread(pool: ForkJoinPool): ForkJoinWorkerThread with BlockContext =
    new ForkJoinWorkerThread(pool) with BlockContext {
      private[this] var isBlocked: Boolean = false // This is only ever read & written if this thread is the current thread
      final override def blockOn[T](thunk: =>T)(implicit permission: CanAwait): T =
        if ((Thread.currentThread eq this) && !isBlocked) {
          try {
            isBlocked = true
            val b: ForkJoinPool.ManagedBlocker with (() => T) =
              new ForkJoinPool.ManagedBlocker with (() => T) {
                private[this] var result: T = null.asInstanceOf[T]
                private[this] var done: Boolean = false
                final override def block(): Boolean = {
                  try {
                    if (!done)
                      result = thunk
                  } finally {
                    done = true
                  }

                  true
                }

                final override def isReleasable = done

                final override def apply(): T = result
              }
            ForkJoinPool.managedBlock(b)
            b()
          } finally {
            isBlocked = false
          }
        } else thunk // Unmanaged blocking
    }

  val longTaskForkJoinPool = new ForkJoinPool(numLongWorkers, (pool: ForkJoinPool) => {
    val worker = createBlockingSupportingForkJoinWorkerThread(pool)
    worker.setName("long-task-worker-" + worker.getPoolIndex)
    worker
  }, null, true)
  
  val longTaskTaskSupport = new ForkJoinTaskSupport(longTaskForkJoinPool)
  val longTaskExecutionContext = ExecutionContext.fromExecutorService(longTaskForkJoinPool)
  
  val shortTaskForkJoinPool = new ForkJoinPool(numShortWorkers, (pool: ForkJoinPool) => {
    val worker = createBlockingSupportingForkJoinWorkerThread(pool)
    worker.setName("short-task-worker-" + worker.getPoolIndex)
    worker
  }, null, true)
  val shortTaskTaskSupport = new ForkJoinTaskSupport(shortTaskForkJoinPool)
  val shortTaskExecutionContext = ExecutionContext.fromExecutorService(shortTaskForkJoinPool)

  BooleanQuery.setMaxClauseCount(Int.MaxValue)

  private val whitespaceAnalyzer = new WhitespaceAnalyzer()

  private val noSimilarity = new SimilarityBase {
    override def score(stats: BasicStats, freq: Double, docLen: Double): Double = 1
    override def toString: String = "NoSimilarity"
  }


  private val termFrequencySimilarity = new SimilarityBase() {
    override def score(stats: BasicStats, freq: Double, docLen: Double): Double = freq
    override def explain(stats: BasicStats, freq: Explanation, docLen: Double): Explanation = Explanation.`match`(freq.getValue,"")
    override def toString: String = "TermFrequencySimilarity"
  }

  private case class TermsEnumToBytesRefIterator(te: BytesRefIterator) extends Iterator[BytesRef] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    override def next(): BytesRef = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      br
    }
    override def hasNext: Boolean = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }
  
  private case class TermsToBytesRefIterable(te: Terms) extends Iterable[BytesRef] {
    def iterator: Iterator[BytesRef] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefIterator(te: BytesRefIterator): Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)

  private case class TermsEnumToBytesRefAndDocFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Int)] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    override def next(): (BytesRef,Int) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.docFreq)
      ret
    }
    override def hasNext: Boolean = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }
  
  private case class TermsToBytesRefAndDocFreqIterable(te: Terms) extends Iterable[(BytesRef,Int)] {
    def iterator: Iterator[(BytesRef,Int)] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefAndDocFreqIterator(te: TermsEnum): Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)

  private case class TermsEnumToBytesRefAndTotalTermFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Long)] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    override def next(): (BytesRef,Long) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.totalTermFreq)
      ret
    }
    override def hasNext: Boolean = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }
  
  private case class TermsToBytesRefAndTotalTermFreqIterable(te: Terms) extends Iterable[(BytesRef,Long)] {
    def iterator: Iterator[(BytesRef,Long)] = te.iterator
  }
  
  private implicit def termsEnumToBytesRefAndTotalTermFreqIterator(te: TermsEnum): Iterator[(BytesRef,Long)] = TermsEnumToBytesRefAndTotalTermFreqIterator(te)
  
  private case class TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te: TermsEnum) extends Iterator[(BytesRef,Int,Long)] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    override def next(): (BytesRef,Int,Long) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.docFreq, te.totalTermFreq)
      ret
    }
    override def hasNext: Boolean = {
      if (!nextFetched) {
        br = te.next()
        nextFetched = true
      }
      br != null
    }
  }

  private case class TermsToBytesRefAndDocFreqAndTotalTermFreqIterable(te: Terms) extends Iterable[(BytesRef,Int,Long)] {
    def iterator: Iterator[(BytesRef,Int,Long)] = te.iterator
  }

  private implicit def termsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te: TermsEnum): Iterator[(BytesRef,Int,Long)] = TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te)

  case class RichBytesRefIterator(te: BytesRefIterator) {
    def asBytesRefIterator: Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)
  }

  implicit def bytesRefIteratorToRichBytesRefIterator(te: BytesRefIterator): RichBytesRefIterator = RichBytesRefIterator(te)

  case class RichTermsEnum(te: TermsEnum) {
    def asBytesRefIterator: Iterator[BytesRef] = TermsEnumToBytesRefIterator(te)
    def asBytesRefAndDocFreqIterator: Iterator[(BytesRef,Int)] = TermsEnumToBytesRefAndDocFreqIterator(te)
    def asBytesRefAndDocFreqAndTotalTermFreqIterator: Iterator[(BytesRef,Int,Long)] = TermsEnumToBytesRefAndDocFreqAndTotalTermFreqIterator(te)
    def asBytesRefAndTotalTermFreqIterator: Iterator[(BytesRef,Long)] = TermsEnumToBytesRefAndTotalTermFreqIterator(te)
  }
  
  implicit def termsEnumToRichTermsEnum(te: TermsEnum): RichTermsEnum = RichTermsEnum(te)
  
  case class RichTerms(te: Terms) {
    def asBytesRefIterable: Iterable[BytesRef] = TermsToBytesRefIterable(te)
    def asBytesRefAndDocFreqIterable: Iterable[(BytesRef,Int)] = TermsToBytesRefAndDocFreqIterable(te)
    def asBytesRefAndTotalTermFreqIterable: Iterable[(BytesRef,Long)] = TermsToBytesRefAndTotalTermFreqIterable(te)
    def asBytesRefAndDocFreqAndTotalTermFreqIterable: Iterable[(BytesRef,Int,Long)] = TermsToBytesRefAndDocFreqAndTotalTermFreqIterable(te)
  }
  
  implicit def termsToRichTerms(te: Terms): RichTerms = RichTerms(te)
  
  def getHitCountForQuery(is: IndexSearcher, q: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Long = {
    (q.rewrite(is.getIndexReader) match {
      case q: ConstantScoreQuery => q.getQuery
      case q => q
    }) match {
      case q: TermQuery => is.getIndexReader.totalTermFreq(q.getTerm)
      case q =>
        val hc = new TotalHitCountCollector()
        tlc.get.setCollector(hc)
        is.search(q,tlc.get)
        hc.getTotalHits

    }
  }
  
}

@Singleton
class IndexAccessProvider @Inject() (config: Configuration,lifecycle: ApplicationLifecycle) extends Logging {
  val indexAccesses = {
    val c = config.get[Configuration]("indices")
    c.keys.map(k => Future {
      val p = c.get[String](k)
      try {
        Some((k, new IndexAccess(k,p,lifecycle)))
      } catch {
        case e: Exception =>
          logger.warn("Couldn't initialise index "+k+" at "+p,e)
          None
      }
    }(IndexAccess.shortTaskExecutionContext)).flatMap(Await.result(_,Duration.Inf)).toMap
  }
  def apply(id: String): IndexAccess = indexAccesses(id)
  def toJson: JsValue = Json.toJson(indexAccesses.view.mapValues(v => v.indexMetadata.indexName))
}

sealed abstract class StoredFieldType extends EnumEntry {
  def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue]
  def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_]
}

object StoredFieldType extends Enum[StoredFieldType] with Logging {
  case object LATLONDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getSortedNumeric(lr, field)
      doc: Int => if (dvs.advanceExact(doc)) {
        val value = dvs.nextValue
        Some(JsObject(Seq("lat"->JsNumber(GeoEncodingUtils.decodeLatitude((value >> 32).toInt)),
          "lon"->JsNumber(GeoEncodingUtils.decodeLongitude((value & 0xFFFFFFFF).toInt)))))
      } else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Long] = {
      val ret = new mutable.HashSet[Long]
      tlc.get.setCollector(new SimpleCollector() {

        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES

        var dv: SortedNumericDocValues = _

        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc))
            ret += this.dv.nextValue()
        }

        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getSortedNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"LatLonDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object NUMERICDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getNumeric(lr, field)
      doc: Int => if (dvs.advanceExact(doc)) Some(JsNumber(dvs.longValue)) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Long] = {
      val ret = new mutable.HashSet[Long]
      tlc.get.setCollector(new SimpleCollector() {

        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES

        var dv: NumericDocValues = _

        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc))
            ret += this.dv.longValue()
        }

        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"NumericDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object FLOATDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getNumeric(lr, field)
      doc: Int => if (dvs.advanceExact(doc)) {
        Some(JsNumber(BigDecimal(java.lang.Float.intBitsToFloat(dvs.longValue.toInt).toDouble))) } else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Float] = {
      val ret = new mutable.HashSet[Float]
      tlc.get.setCollector(new SimpleCollector() {

        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES

        var dv: NumericDocValues = _

        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc))
            ret += java.lang.Float.intBitsToFloat(this.dv.longValue.toInt)
        }

        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"FloatDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object DOUBLEDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getNumeric(lr, field)
      doc: Int => if (dvs.advanceExact(doc)) Some(JsNumber(BigDecimal(java.lang.Double.longBitsToDouble(dvs.longValue)))) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Double] = {
      val ret = new mutable.HashSet[Double]
      tlc.get.setCollector(new SimpleCollector() {

        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES

        var dv: NumericDocValues = _

        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc))
            ret += java.lang.Double.longBitsToDouble(this.dv.longValue)
        }

        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"DoubleDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object SORTEDDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getSorted(lr, field)
      if (containsJson)
        (doc: Int) => if (dvs.advanceExact(doc)) Some(Json.parse(dvs.binaryValue.utf8ToString())) else None
      else
        (doc: Int) => if (dvs.advanceExact(doc)) Some(JsString(dvs.binaryValue.utf8ToString())) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[BytesRef] = {
      val ret = new mutable.HashSet[BytesRef]
      tlc.get.setCollector(new SimpleCollector() {
        
        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
        
        var dv: SortedDocValues = _
  
        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc))
            ret += BytesRef.deepCopyOf(this.dv.binaryValue())
        }
        
        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getSorted(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"SortedDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }    
  case object SORTEDNUMERICDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getSortedNumeric(lr, field)
      doc: Int => if (dvs.advanceExact(doc)) Some({
        val values = new Array[JsValue](dvs.docValueCount)
        var i = 0
        while (i<values.length) {
          values(i)=JsNumber(dvs.nextValue)
          i += 1
        }
        JsArray(values)
      }) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Long] = {
      val ret = new mutable.HashSet[Long]
      tlc.get.setCollector(new SimpleCollector() {
        
        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
        
        var dv: SortedNumericDocValues = _
  
        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc)) {
            var i = 0
            while (i<values.length) {
              ret += this.dv.nextValue()
              i += 1
            }
          }
        }
        
        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getSortedNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"SortedNumericDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }        
  }
  case object SORTEDSETDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val dvs = DocValues.getSortedSet(lr, field)
      if (containsJson)
        (doc: Int) => if (dvs.advanceExact(doc)) Some({
          val values = new ArrayBuffer[JsValue]
          var ord = dvs.nextOrd
          while (ord != SortedSetDocValues.NO_MORE_ORDS) {
            values += Json.parse(dvs.lookupOrd(ord).utf8ToString)
            ord = dvs.nextOrd
          }
          JsArray(values)
        }) else None
      else
        (doc: Int) => if (dvs.advanceExact(doc)) Some({
          val values = new ArrayBuffer[JsValue]
          var ord = dvs.nextOrd
          while (ord != SortedSetDocValues.NO_MORE_ORDS) {
            values += JsString(dvs.lookupOrd(ord).utf8ToString)
            ord = dvs.nextOrd
          }
          JsArray(values)
        }) else None

    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[BytesRef] = {
      val ret = new mutable.HashSet[BytesRef]
      tlc.get.setCollector(new SimpleCollector() {
        
        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
        
        var dv: SortedSetDocValues = _
  
        override def collect(doc: Int): Unit = {
          if (this.dv.advanceExact(doc)) {
            var ord = this.dv.nextOrd
            while (ord != SortedSetDocValues.NO_MORE_ORDS) {
              ret += BytesRef.deepCopyOf(this.dv.lookupOrd(ord))
              ord = this.dv.nextOrd
            }
          }
        }
        
        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.dv = DocValues.getSortedSet(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"SortedSetDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object SINGULARSTOREDFIELD extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val fieldS = Collections.singleton(field)
      if (containsJson)
        (doc: Int) => Option(lr.document(doc,fieldS).get(field)).map(Json.parse)
      else
        (doc: Int) => Option(lr.document(doc,fieldS).get(field)).map(JsString)
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[BytesRef] = {
      val ret = new mutable.HashSet[BytesRef]
      val fieldS = Collections.singleton(field)
      tlc.get.setCollector(new SimpleCollector() {
        
        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
        
        var r: LeafReader = _
  
        override def collect(doc: Int): Unit = {
          val fv = r.document(doc, fieldS).getBinaryValue(field)
          if (fv != null) ret += BytesRef.deepCopyOf(fv)
        }
        
        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.r = context.reader
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"SingularStoredField -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object MULTIPLESTOREDFIELDS extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = {
      val fieldS = Collections.singleton(field)
      if (containsJson)
        (doc: Int) => {
          val values = lr.document(doc,fieldS).getValues(field).map(Json.parse)
          if (values.isEmpty) None else Some(JsArray(values))
        }
      else
        (doc: Int) => {
          val values = lr.document(doc,fieldS).getValues(field).map(JsString)
          if (values.isEmpty) None else Some(JsArray(values))
        }
    }    
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[BytesRef] = {
      val ret = new mutable.HashSet[BytesRef]
      val fieldS = Collections.singleton(field)
      tlc.get.setCollector(new SimpleCollector() {
        
        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
        
        var r: LeafReader = _
  
        override def collect(doc: Int): Unit = {
          for (fv <- r.document(doc, fieldS).getBinaryValues(field)) ret += BytesRef.deepCopyOf(fv)
        }
        
        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.r = context.reader
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"MultiStoredField -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object TERMVECTOR extends StoredFieldType {
    import IndexAccess._
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] =
      if (containsJson)
        (doc: Int) => {
          val values = lr.getTermVector(doc, field).asBytesRefIterable.map(br => Json.parse(br.utf8ToString)).toSeq
          if (values.isEmpty) None else Some(JsArray(values))
        }
      else
        (doc: Int) => {
          val values = lr.getTermVector(doc, field).asBytesRefIterable.map(br => JsString(br.utf8ToString)).toSeq
          if (values.isEmpty) None else Some(JsArray(values))
        }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[BytesRef] = {
      val ret = new mutable.HashSet[BytesRef]
      tlc.get.setCollector(new SimpleCollector() {
        
        override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
        
        var r: LeafReader = _
  
        override def collect(doc: Int): Unit = {
          r.getTermVector(doc, field).asBytesRefIterable.foreach(ret += BytesRef.deepCopyOf(_))
        }
        
        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.r = context.reader
        }
      })
      is.search(q, tlc.get)
      logger.debug(f"Termvector -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object NONE extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): Int => Option[JsValue] = throw new IllegalArgumentException
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_] = throw new IllegalArgumentException
  }
  val values = findValues
}

sealed abstract class IndexedFieldType extends EnumEntry {
}

object IndexedFieldType extends Enum[IndexedFieldType] {
  case object TEXT extends IndexedFieldType
  case object STRING extends IndexedFieldType
  case object INTPOINT extends IndexedFieldType
  case object LONGPOINT extends IndexedFieldType
  case object FLOATPOINT extends IndexedFieldType
  case object DOUBLEPOINT extends IndexedFieldType
  case object LATLONPOINT extends IndexedFieldType
  case object NONE extends IndexedFieldType
  val values = findValues
}

object TermSort extends Enumeration {
  val TERM,DOCFREQ,TERMFREQ = Value
}

case class MetadataOpts(
  stats: Boolean,
  quantiles: Boolean,
  histograms: Boolean,
  from: BigDecimal,
  to: BigDecimal,
  by: BigDecimal,
  maxTermsToStat: Int,
  sortTermsBy: TermSort.Value,
  sortTermDirection: SortDirection.Value,
  length: Int
) {
  val formatString: String = "%."+length+"f"
}


case class FieldInfo(
  id: String,
  description: String,
  storedAs: StoredFieldType,
  indexedAs: IndexedFieldType,
  containsJson: Boolean
) {
  def getStats(lr: LeafReader, stats: mutable.HashMap[String,(TDigest,TDigest)], metadataOpts: MetadataOpts): JsObject = {
    var ret = indexedAs match {
      case IndexedFieldType.TEXT | IndexedFieldType.STRING =>
        val terms = lr.terms(id)
        if (terms==null) Json.obj()
        else {
          var ret = if (!metadataOpts.stats) Json.obj()
            else Json.obj(
            "totalDocs" -> terms.getDocCount,
            "totalTerms" -> terms.size,
            "sumDocFreq" -> terms.getSumDocFreq,
            "sumTotalTermFreq" -> terms.getSumTotalTermFreq)
          if (terms.size<=metadataOpts.maxTermsToStat) {
            import IndexAccess._
            var stats = terms.asBytesRefAndDocFreqAndTotalTermFreqIterable.map(t => (t._1.utf8ToString(),t._2,t._3)).toSeq
            metadataOpts.sortTermsBy match {
              case TermSort.TERM => stats = stats.sortBy(_._1)
              case TermSort.DOCFREQ => stats = stats.sortBy(_._2)
              case TermSort.TERMFREQ => stats = stats.sortBy(_._3)
            }
            if (metadataOpts.sortTermDirection==SortDirection.DESC) stats = stats.reverse
            ret = ret ++ Json.obj("termFreqs"->stats.map(t => Json.obj("term"->t._1,"docFreq"->t._2,"totalTermFreq"->t._3)))
          }
          if (metadataOpts.quantiles && stats.contains(id)) ret = ret ++ {
            val (ttft, dft) = stats(id)
            Json.obj(
              "termFreqQuantiles" -> (metadataOpts.from to metadataOpts.to by metadataOpts.by).map(q => Json.obj("quantile" -> metadataOpts.formatString.formatLocal(Locale.ROOT,q), "freq" -> ttft.quantile(Math.min(q.doubleValue, 1.0)).toLong)),
              "docFreqQuantiles" -> (metadataOpts.from to metadataOpts.to by metadataOpts.by).map(q => Json.obj("quantile" -> metadataOpts.formatString.formatLocal(Locale.ROOT,q), "freq" -> dft.quantile(Math.min(q.doubleValue, 1.0)).toLong))
            )
          }
          ret
        }
      case IndexedFieldType.INTPOINT | IndexedFieldType.LONGPOINT | IndexedFieldType.FLOATPOINT | IndexedFieldType.DOUBLEPOINT | IndexedFieldType.LATLONPOINT =>
        val pv = lr.getPointValues(id)
        if (pv == null || !metadataOpts.stats) Json.obj()
        else
          Json.obj(
            "totalDocs" -> pv.getDocCount,
            "sumTotalTermFreq" -> pv.size,
          ) ++ (indexedAs match {
            case IndexedFieldType.INTPOINT => Json.obj("min" -> IntPoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> IntPoint.decodeDimension(pv.getMaxPackedValue, 0))
            case IndexedFieldType.LONGPOINT => Json.obj("min" -> LongPoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> LongPoint.decodeDimension(pv.getMaxPackedValue, 0))
            case IndexedFieldType.FLOATPOINT => Json.obj("min" -> FloatPoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> FloatPoint.decodeDimension(pv.getMaxPackedValue, 0))
            case IndexedFieldType.DOUBLEPOINT => Json.obj("min" -> DoublePoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> DoublePoint.decodeDimension(pv.getMaxPackedValue, 0))
            case IndexedFieldType.LATLONPOINT => Json.obj("min" -> Json.obj("lat" -> GeoEncodingUtils.decodeLatitude(pv.getMinPackedValue, 0), "lon" -> GeoEncodingUtils.decodeLongitude(pv.getMinPackedValue, Integer.BYTES)), "max" -> Json.obj("lat" -> GeoEncodingUtils.decodeLatitude(pv.getMaxPackedValue, 0), "lon" -> GeoEncodingUtils.decodeLongitude(pv.getMaxPackedValue, Integer.BYTES)))
            case _ => throw new IllegalStateException("There should be a guard above this to prevent us from ever reaching here")
          })
      case IndexedFieldType.NONE => Json.obj()
    }
    if ((metadataOpts.quantiles || metadataOpts.histograms) && stats.contains(id)) storedAs match {
      case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
        val (histogram, _) = stats(id)
        if (metadataOpts.quantiles) ret = ret ++ Json.obj(
          "quantiles" -> (metadataOpts.from to metadataOpts.to by metadataOpts.by).map(q => Json.obj("quantile" -> metadataOpts.formatString.formatLocal(Locale.ROOT,q), "max" -> histogram.quantile(Math.min(q.doubleValue, 1.0)).toLong))
        )
        if (metadataOpts.histograms) {
          var min = histogram.getMin.toLong
          var max = histogram.getMax.toLong
          val distance = max-min
          min = min + (metadataOpts.from.toDouble*distance).toLong
          max = Math.min(min + (metadataOpts.to.toDouble*distance).toLong,max)
          val step = (max-min)/Math.max(((metadataOpts.to.toDouble-metadataOpts.from.toDouble)/metadataOpts.by.toDouble).toLong,1)+1
          ret = ret ++ Json.obj(
            "histogram" -> (min to max by step).map(q => Json.obj("min" -> q, "max" -> (q+step), "proportion" -> (histogram.cdf((q+step).toDouble) - histogram.cdf(q.toDouble))))
          )
        }
      case StoredFieldType.FLOATDOCVALUES | StoredFieldType.DOUBLEDOCVALUES =>
        val (histogram, _) = stats(id)
        if (metadataOpts.quantiles) ret = ret ++ Json.obj(
          "quantiles" -> (metadataOpts.from to metadataOpts.to by metadataOpts.by).map(q => Json.obj("quantile" -> metadataOpts.formatString.formatLocal(Locale.ROOT,q), "max" -> histogram.quantile(Math.min(q.doubleValue, 1.0))))
        )
        if (metadataOpts.histograms) {
          var min = histogram.getMin
          var max = histogram.getMax
          val distance = max-min
          min = min + (metadataOpts.from.toDouble*distance)
          max = Math.min(min + (metadataOpts.to.toDouble*distance),max)
          val step = (max-min)/Math.max((metadataOpts.to.toDouble-metadataOpts.from.toDouble)/metadataOpts.by.toDouble,Double.MinPositiveValue)
          ret = ret ++ Json.obj(
            "histogram" -> (BigDecimal(min) to BigDecimal(max) by BigDecimal(step)).map(_.toDouble).map(q => Json.obj("min" -> q, "max" -> (q+step), "proportion" -> (histogram.cdf(q+step) - histogram.cdf(q))))
          )
        }
      case StoredFieldType.LATLONDOCVALUES =>
        val (lathistogram, lonhistogram) = stats(id)
        if (metadataOpts.quantiles) ret = ret ++ Json.obj(
          "latquantiles" -> (metadataOpts.from to metadataOpts.to by metadataOpts.by).map(q => Json.obj("quantile" -> metadataOpts.formatString.formatLocal(Locale.ROOT,q), "max" -> lathistogram.quantile(Math.min(q.doubleValue, 1.0)))),
          "lonquantiles" -> (metadataOpts.from to metadataOpts.to by metadataOpts.by).map(q => Json.obj("quantile" -> metadataOpts.formatString.formatLocal(Locale.ROOT,q), "max" -> lonhistogram.quantile(Math.min(q.doubleValue, 1.0))))
        )
        if (metadataOpts.histograms) {
          var min = lathistogram.getMin
          var max = lathistogram.getMax
          var distance = max-min
          min = min + (metadataOpts.from.toDouble*distance)
          max = Math.min(min + (metadataOpts.to.toDouble*distance),max)
          var step = (max-min)/Math.max((metadataOpts.to.toDouble-metadataOpts.from.toDouble)/metadataOpts.by.toDouble,Double.MinPositiveValue)
          ret = ret ++ Json.obj(
            "lathistogram" -> (BigDecimal(min) to BigDecimal(max) by BigDecimal(step)).map(_.toDouble).map(q => Json.obj("min" -> q, "max" -> (q+step), "proportion" -> (lathistogram.cdf(q+step) - lathistogram.cdf(q))))
          )
          min = lonhistogram.getMin
          max = lonhistogram.getMax
          distance = max-min
          min = min + (metadataOpts.from.toDouble*distance)
          max = Math.min(min + (metadataOpts.to.toDouble*distance),max)
          step = (max-min)/Math.max((metadataOpts.to.toDouble-metadataOpts.from.toDouble)/metadataOpts.by.toDouble,Double.MinPositiveValue)
          ret = ret ++ Json.obj(
            "lonhistogram" -> (BigDecimal(min) to BigDecimal(max) by BigDecimal(step)).map(_.toDouble).map(q => Json.obj("min" -> q, "max" -> (q+step), "proportion" -> (lonhistogram.cdf(q+step) - lonhistogram.cdf(q))))
          )
        }
      case StoredFieldType.NONE | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.TERMVECTOR | StoredFieldType.SORTEDSETDOCVALUES =>
    }
    ret
  }
  def getMatchingValues(is: IndexSearcher, q: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_] = storedAs.getMatchingValues(is, q, id)
  def jsGetter(lr: LeafReader): Int => Option[JsValue] = storedAs.jsGetter(lr, id, containsJson)
  def toJson(lr: LeafReader, stats: mutable.HashMap[String,(TDigest,TDigest)], metadataOpts: MetadataOpts) = {
    val ret = Json.obj(
      "description" -> description,
      "storedAs" -> storedAs.toString,
      "indexedAs" -> indexedAs.toString,
      "containsJson" -> containsJson)
    if (!metadataOpts.stats && !metadataOpts.quantiles && !metadataOpts.histograms) ret else ret ++ getStats(lr, stats, metadataOpts)
    }
}

case class LevelMetadata(
  id: String,
  description: String,
  idField: String,
  indices: Seq[String],
  preload: Boolean,
  fields: Map[String,FieldInfo]) {
  val idFieldAsTerm = new Term(idField,"")
  val idFieldInfo: FieldInfo = fields(idField)
  val fieldStats = new mutable.HashMap[String,(TDigest,TDigest)]
  def ensureFieldStats(path: String, lr: LeafReader, logger: Logger): Unit =
    for ((name, info) <- fields) {
      new File(path+"/stats/"+id).mkdirs()
      if (!new File(path+"/stats/"+id).exists) throw new IllegalArgumentException("Could not create directory "+path+"/stats/"+id)
      val fname = path + "/stats/" + id + "/" + name + ".stats"
      if (new File(fname).exists) {
        val f = new RandomAccessFile(fname, "r")
        val inChannel = f.getChannel
        val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)
        val d1 = MergingDigest.fromBytes(buffer)
        val d2 = if (buffer.position() < inChannel.size) MergingDigest.fromBytes(buffer) else null
        fieldStats.put(name, (d1, d2))
        inChannel.close()
        f.close()
      } else {
        info.indexedAs match {
          case IndexedFieldType.TEXT | IndexedFieldType.STRING =>
            logger.info(f"Stats for $fname of type ${info.indexedAs} not found. Calculating.")
            val dft = TDigest.createMergingDigest(100)
            val ttft = TDigest.createMergingDigest(100)
            import IndexAccess._
            val terms = lr.terms(name)
            if (terms!=null) {
              for ((_, df, ttf) <- terms.asBytesRefAndDocFreqAndTotalTermFreqIterable) {
                dft.add(df)
                ttft.add(ttf.toDouble)
              }
              val bb = ByteBuffer.allocateDirect(dft.smallByteSize + ttft.smallByteSize)
              dft.asSmallBytes(bb)
              ttft.asSmallBytes(bb)
              bb.flip()
              val out = new FileOutputStream(fname)
              out.getChannel.write(bb)
              out.close()
              fieldStats.put(name, (dft, ttft))
              logger.info(f"Finished calculating stats for $fname.")
            }
          case _ =>
            info.storedAs match {
              case StoredFieldType.NONE | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.TERMVECTOR | StoredFieldType.SORTEDSETDOCVALUES =>
              case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES | StoredFieldType.DOUBLEDOCVALUES | StoredFieldType.FLOATDOCVALUES =>
                logger.info(f"Stats for $fname of type ${info.storedAs} not found. Calculating.")
                val histogram = TDigest.createMergingDigest(100)
                info.storedAs match {
                  case StoredFieldType.NUMERICDOCVALUES =>
                    val dv = lr.getNumericDocValues(name)
                    var values = 0
                    if (dv!=null) while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                      values += 1
                      histogram.add(dv.longValue.toDouble)
                    }
                    histogram.add(0,lr.numDocs-values)
                  case StoredFieldType.SORTEDNUMERICDOCVALUES =>
                    val dv = lr.getSortedNumericDocValues(name)
                    var values = 0
                    if (dv!=null) while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                      values += 1
                      for (_ <- 0 to dv.docValueCount)
                        histogram.add(dv.nextValue.toDouble)
                    }
                    histogram.add(0,lr.numDocs-values)
                  case StoredFieldType.FLOATDOCVALUES =>
                    val dv = lr.getNumericDocValues(name)
                    var values = 0
                    if (dv!=null) while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                      values += 1
                      histogram.add(java.lang.Float.intBitsToFloat(dv.longValue.toInt))
                    }
                    histogram.add(0,lr.numDocs-values)
                  case StoredFieldType.DOUBLEDOCVALUES =>
                    val dv = lr.getNumericDocValues(name)
                    var values = 0
                    if (dv!=null) while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                      values += 1
                      histogram.add(java.lang.Double.longBitsToDouble(dv.longValue))
                    }
                    histogram.add(0,lr.numDocs-values)
                  case _ => throw new IllegalStateException("There should be a guard above this to prevent us from ever reaching here")
                }
                val bb = ByteBuffer.allocateDirect(histogram.smallByteSize)
                histogram.asSmallBytes(bb)
                bb.flip()
                val out = new FileOutputStream(fname)
                out.getChannel.write(bb)
                out.close()
                fieldStats.put(name,(histogram,null))
                logger.info(f"Finished calculating stats for $fname.")
              case StoredFieldType.LATLONDOCVALUES =>
                logger.info(f"Stats for $fname of type ${info.storedAs} not found. Calculating.")
                val lathistogram = TDigest.createDigest(100)
                val lonhistogram = TDigest.createDigest(100)
                val dvs = DocValues.getSortedNumeric(lr, name)
                if (dvs!=null) while (dvs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                  val value = dvs.nextValue
                  lathistogram.add(GeoEncodingUtils.decodeLatitude((value >> 32).toInt))
                  lonhistogram.add(GeoEncodingUtils.decodeLongitude((value & 0xFFFFFFFF).toInt))
                }
                val bb = ByteBuffer.allocateDirect(lathistogram.smallByteSize + lonhistogram.smallByteSize)
                lathistogram.asSmallBytes(bb)
                lonhistogram.asSmallBytes(bb)
                bb.flip()
                val out = new FileOutputStream(fname)
                out.getChannel.write(bb)
                out.close()
                fieldStats.put(name,(lathistogram,lonhistogram))
                logger.info(f"Finished calculating stats for $fname.")
            }
        }
      }
    }
  def toJson(ia: IndexAccess, metadataOpts: MetadataOpts, commonFields: Set[FieldInfo]): JsValue = Json.obj(
    "id"->id,
    "description"->description,
    "idField"->idField,
    "indices"->indices,
    "preload"->preload,
    "documents"->ia.reader(id).maxDoc(),
    "fields"->fields.filter(p => metadataOpts.stats || metadataOpts.histograms || metadataOpts.quantiles || !commonFields.contains(p._2)).view.mapValues(_.toJson(ia.reader(id).leaves.get(0).reader,fieldStats,metadataOpts)).toMap[String,JsValue]
  )
  def getMatchingValues(is: IndexSearcher, q: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_] = fields(idField).getMatchingValues(is, q)
}

object OffsetSearchType extends Enumeration {
  val EXACT, PREV, NEXT = Value
}

case class IndexMetadata(
  indexId: String,
  indexName: String,
  indexVersion: String,
  indexType: String,
  auths: Map[String,String],
  contentField: String,
  contentTokensField: String,
  levels: Seq[LevelMetadata],
  defaultLevelS: Option[String],
  indexingAnalyzersAsText: Map[String,String],
  offsetDataConverterLang: Option[String],
  offsetDataConverterAsText: Option[String]
) {
  val directoryCreator: Path => Directory = (path: Path) => indexType match {
    case "MMapDirectory" =>
      new MMapDirectory(path)        
    case "SimpleFSDirectory" => new SimpleFSDirectory(path)
    case "NIOFSDirectory" => new NIOFSDirectory(path)
    case any => throw new IllegalArgumentException("Unknown directory type "+any)
  }
  val indexingAnalyzers: Map[String,Analyzer] = indexingAnalyzersAsText.view.mapValues{
    case "OctavoAnalyzer" => IndexAccess.octavoAnalyzer
    case "StandardAnalyzer" => new StandardAnalyzer(CharArraySet.EMPTY_SET)
    case a if a.startsWith("MorphologicalAnalyzer_") => new MorphologicalAnalyzer(new Locale(a.substring(22)))
    case a if a.startsWith("CustomAnalyzer") =>
      val a2 = a.substring(15)
      val ci = a2.indexOf(':')
      val ase = scriptEngineManager.getEngineByName(a2.substring(0,ci))
      ase.eval(a2.substring(ci+1)).asInstanceOf[Analyzer]
    case any => throw new IllegalArgumentException("Unknown analyzer type " + any)
  }.toMap.withDefaultValue(new KeywordAnalyzer())
  val levelOrder: Map[String,Int] = levels.map(_.id).zipWithIndex.toMap
  val levelMap: Map[String,LevelMetadata] = levels.map(l => (l.id,l)).toMap
  val defaultLevel: LevelMetadata = defaultLevelS.map(levelMap(_)).getOrElse(levels.last)
  private val offsetDataConverterEngine: ScriptEngine with Invocable = scriptEngineManager.getEngineByName(offsetDataConverterLang.getOrElse("Groovy")).asInstanceOf[ScriptEngine with Invocable]
  val offsetDataConverter: (Cursor,ByteIterable,OffsetSearchType.Value) => JsValue = if (offsetDataConverterAsText.isDefined) {
    offsetDataConverterEngine.eval(offsetDataConverterAsText.get)
    (cursor: Cursor, key: ByteIterable, offsetSearchType: OffsetSearchType.Value) =>
      offsetDataConverterEngine.invokeFunction("convert",cursor,key,Int.box(offsetSearchType.id)) match {
        case null => JsNull
        case value: JsValue => value
        case value: java.util.Map[String @unchecked, JsValue @unchecked] => Json.toJson(value.asScala)
      }
  } else null
  val commonFields: Map[String, FieldInfo] = levels.map(_.fields).reduce((l, r) => l.filter(lp => r.get(lp._1).contains(lp._2)))
  def toJson(ia: IndexAccess, metadataOpts: MetadataOpts) = Json.obj(
    "name"->indexName,
    "version"->indexVersion,
    "indexType"->indexType,
    "contentField"->contentField,
    "contentTokensField"->contentTokensField,
    "levels"->levels.map(_.toJson(ia,metadataOpts,commonFields.values.toSet)),
    "defaultLevel"->defaultLevel.id,
    "indexingAnalyzers"->indexingAnalyzersAsText,
    "commonFields"->commonFields.view.mapValues(_.toJson(ia.reader(defaultLevel.id).leaves.get(0).reader,defaultLevel.fieldStats,metadataOpts)).toMap[String,JsValue]
  )
}

class IndexAccess(id: String, path: String, lifecycle: ApplicationLifecycle) extends Logging {
  
  import IndexAccess._
 
  private val readers = new mutable.HashMap[String,IndexReader]
  private val tfSearchers = new mutable.HashMap[String,IndexSearcher]
  private val bm25Searchers = new mutable.HashMap[String,IndexSearcher]
  private val noSearchers = new mutable.HashMap[String,IndexSearcher]
  
  def readFieldInfos(
      c: Map[String,JsValue]): Map[String,FieldInfo] = c.map{ case (fieldId,v) => (fieldId,FieldInfo(fieldId,(v \ "description").as[String], StoredFieldType.withName((v \ "storedAs").asOpt[String].getOrElse("NONE").toUpperCase) , IndexedFieldType.withName((v \ "indexedAs").asOpt[String].getOrElse("NONE").toUpperCase), (v \ "containsJson").asOpt[Boolean].getOrElse(false))) }
  
  def readLevelMetadata(
    c: JsValue,
    commonFields: Map[String,FieldInfo]
  ) = LevelMetadata(
      (c \ "id").as[String],
      (c \ "description").as[String],
      (c \ "term").as[String],
      (c \ "index").asOpt[String].toSeq ++ (c \ "indices").asOpt[Seq[String]].getOrElse(Seq.empty),
      (c \ "preload").asOpt[Boolean].getOrElse(false),
      (c \ "fields").asOpt[Map[String,JsValue]].map(readFieldInfos).getOrElse(Map.empty) ++ commonFields
  )
  
  def readIndexMetadata(c: JsValue) = IndexMetadata(id,
    (c \ "name").as[String],
    (c \ "version").as[String],
    (c \ "indexType").asOpt[String].getOrElse("MMapDirectory"),
    (c \ "auths").asOpt[Map[String,String]].getOrElse(Map.empty),
    (c \ "contentField").as[String],
    (c \ "contentTokensField").as[String],
    (c \ "levels").as[Seq[JsValue]].map(readLevelMetadata(_,
      (c \ "commonFields").asOpt[Map[String,JsValue]].map(readFieldInfos).getOrElse(Map.empty)
    )),
    (c \ "defaultLevel").asOpt[String],
    (c \ "indexingAnalyzers").asOpt[Map[String,String]].getOrElse(Map.empty),
    (c \ "offsetDataConverterLang").asOpt[String],
    (c \ "offsetDataConverter").asOpt[String]
  )

  val indexMetadata: IndexMetadata = readIndexMetadata(Json.parse(new FileInputStream(new File(path+"/indexmeta.json"))))

  private val offsetDataEnv = if (indexMetadata.offsetDataConverterAsText.isDefined) Environments.newContextualInstance(path+"/offsetdata", new EnvironmentConfig().setEnvIsReadonly(true).setLogFileSize(Int.MaxValue+1L).setMemoryUsage(Math.min(1073741824L, Runtime.getRuntime.maxMemory/4)).setEnvCloseForcedly(true)) else null
  if (indexMetadata.offsetDataConverterAsText.isDefined && lifecycle != null) lifecycle.addStopHook{() => Future.successful(offsetDataEnv.close())}
  private val offsetDataCursor: ThreadLocal[Cursor] = ThreadLocal.withInitial(() => {
    offsetDataEnv.beginReadonlyTransaction()
    val s = offsetDataEnv.openStore("offsetdata", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    s.openCursor()
  })


  try {
    val futures = for (level <- indexMetadata.levels) yield Future {
      logger.info("Initializing index at "+path+"/["+level.indices.mkString(", ")+"]")
      val seenFields = new collection.mutable.HashSet[String]()
      val mreaders = level.indices.map(index => {
        val directory = indexMetadata.directoryCreator(FileSystems.getDefault.getPath(path+"/"+index))
        if (level.preload && directory.isInstanceOf[MMapDirectory]) directory.asInstanceOf[MMapDirectory].setPreload(true)
        val ret = DirectoryReader.open(directory)
        for (fi <- ret.leaves().get(0).reader().getFieldInfos.iterator.asScala)
          if (!level.fields.contains(fi.name)) logger.warn(s"Encountered undocumented field ${fi.name} in ${path + "/" + index}. Won't know how to handle/publicise it.")
          else seenFields += fi.name
        ret
      })
      val reader = if (mreaders.length == 1) mreaders.head else new ParallelCompositeReader(mreaders:_*)
      level.ensureFieldStats(path,reader.leaves.get(0).reader,logger)
      for (field <- level.fields)
        if (!seenFields.contains(field._1)) logger.warn("Documented field "+ field._1 + " not found in "+path+"/["+level.indices.mkString(", ")+"]")
      readers.put(level.id, reader)
      val tfSearcher = new IndexSearcher(reader)
      tfSearcher.setSimilarity(termFrequencySimilarity)
      tfSearchers.put(level.id, tfSearcher)
      val noSearcher = new IndexSearcher(reader)
      noSearcher.setSimilarity(noSimilarity)
      noSearchers.put(level.id, noSearcher)
      bm25Searchers.put(level.id, new IndexSearcher(reader))
      logger.info("Initialized index at "+path+"/["+level.indices.mkString(", ")+"]")
      (level.id, reader)
    }(shortTaskExecutionContext)
    futures.foreach(Await.result(_, Duration.Inf))
  } catch {
    case e: Exception =>
      logger.error("Encountered an exception initializing index at "+path,e)
      throw e
  }

  def offsetDataGetter: (Int,Int,OffsetSearchType.Value) => JsValue = {
    if (indexMetadata.offsetDataConverterAsText.isEmpty) return (_: Int, _: Int, _: OffsetSearchType.Value) => JsNull
    val cursor = offsetDataCursor.get
    val klos = new LightOutputStream()
    (doc: Int, offset: Int, searchType: OffsetSearchType.Value) => {
      klos.clear()
      IntegerBinding.writeCompressed(klos, doc)
      IntegerBinding.writeCompressed(klos, offset)
      val key = klos.asArrayByteIterable()
      cursor.getSearchKeyRange(key)
      indexMetadata.offsetDataConverter(cursor,key,searchType)
    }
  }

  def offsetDataIterator(doc: Int): Iterator[(Int,JsValue)] = {
    if (indexMetadata.offsetDataConverterAsText.isEmpty) return Iterator.empty
    val cursor = offsetDataCursor.get
    val klos = new LightOutputStream()
    IntegerBinding.writeCompressed(klos, doc)
//    IntegerBinding.writeCompressed(klos, 0)
    val key = klos.asArrayByteIterable()
    var cursorOnNext = cursor.getSearchKeyRange(key) != null
    new Iterator[(Int,JsValue)] {
      override def hasNext = {
        if (!cursorOnNext) cursorOnNext = cursor.getNext
        if (!cursor.getKey.subIterable(0,key.getLength).equals(key)) cursorOnNext = false
        cursorOnNext
      }

      override def next() = {
        if (!cursorOnNext) cursor.getNext
        else cursorOnNext = false
        (IntegerBinding.readCompressed(cursor.getKey.subIterable(key.getLength,cursor.getKey.getLength-key.getLength).iterator()),indexMetadata.offsetDataConverter(cursor,cursor.getKey,OffsetSearchType.EXACT))
      }
    }
  }

  def reader(level: String): IndexReader = readers(level)
  
  def searcher(level: String, queryScoring: QueryScoring.Value): IndexSearcher = {
    queryScoring match {
      case QueryScoring.NONE =>
        noSearchers(level)
      case QueryScoring.BM25 =>
        bm25Searchers(level)
      case QueryScoring.TF =>
        tfSearchers(level)
    }
  }

  val queryAnalyzers = indexMetadata.levels.map(level => (level.id, new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),
      level.fields.values.filter(_.indexedAs == IndexedFieldType.TEXT).map(_.id).map((_,whitespaceAnalyzer)).toMap[String,Analyzer].asJava))).toMap

  private def newQueryParser(level: LevelMetadata): QueryParser = {
    val qp = new ComplexPhraseQueryParser(indexMetadata.contentField,queryAnalyzers(level.id)) {

      override def addClause(clauses: java.util.List[BooleanClause], conj: Int, mods: Int, q: Query): Unit = {
        super.addClause(clauses, conj, mods, q)
        if (mods == 11) {// QueryParserBase.MOD_REQ. Here, we're changing "AND +" into a FILTER, so it doesn't increase score
          val c = clauses.get(clauses.size - 1)
          if (!c.isProhibited)
            clauses.set(clauses.size() - 1, new BooleanClause(c.getQuery, Occur.FILTER))
        }
      }

      override def getWildcardQuery(field: String, term: String): Query = {
        if (term=="*") level.fields.get(field).map(_.indexedAs).getOrElse(IndexedFieldType.NONE) match {
          case IndexedFieldType.INTPOINT =>
            IntPoint.newRangeQuery(field, Int.MinValue, Int.MaxValue)
          case IndexedFieldType.FLOATPOINT =>
            FloatPoint.newRangeQuery(field, Float.NegativeInfinity, Float.PositiveInfinity)
          case IndexedFieldType.DOUBLEPOINT =>
            DoublePoint.newRangeQuery(field, Double.NegativeInfinity, Double.PositiveInfinity)
          case IndexedFieldType.LONGPOINT =>
            LongPoint.newRangeQuery(field,Long.MinValue,Long.MaxValue)
          case IndexedFieldType.LATLONPOINT =>
            LatLonPoint.newBoxQuery(field,Double.NegativeInfinity,Double.PositiveInfinity,Double.NegativeInfinity,Double.PositiveInfinity)
          case _ => super.getWildcardQuery(field,term)
        }
        else super.getWildcardQuery(field,term)
      }
      override def getFieldQuery(field: String, content: String, slop: Int): Query =
        level.fields.get(field).map(_.indexedAs).getOrElse(IndexedFieldType.NONE) match {
          case IndexedFieldType.STRING if slop == 0 => getFieldQuery(field,content,quoted = true)
          case _ if slop <0 =>
            super.setInOrder(false)
            val q = super.getFieldQuery(field,content,-slop - 1)
            super.setInOrder(true)
            q
          case _ => super.getFieldQuery(field,content,slop)
        }
      override def getFieldQuery(field: String, content: String, quoted: Boolean): Query = {
        val ext = field.indexOf("|")
        if (ext == -1)
          level.fields.get(field).map(_.indexedAs).getOrElse(IndexedFieldType.NONE) match {
            case IndexedFieldType.INTPOINT =>
              val value = content.toInt
              IntPoint.newRangeQuery(field, value, value)
            case IndexedFieldType.FLOATPOINT =>
              val value = content.toFloat
              FloatPoint.newRangeQuery(field, value, value)
            case IndexedFieldType.DOUBLEPOINT =>
              val value = content.toDouble
              DoublePoint.newRangeQuery(field, value, value)
            case IndexedFieldType.LONGPOINT =>
              val value = if (content.contains("-"))
                ISODateTimeFormat.dateOptionalTimeParser().parseMillis(content)
              else content.toLong
              LongPoint.newRangeQuery(field, value, value)
            case IndexedFieldType.LATLONPOINT =>
              val parts = content.split(",")
              parts.length match {
                case 3 => LatLonPoint.newDistanceQuery(field, parts.head.toDouble, parts(1).toDouble, parts.last.toDouble)
                case 4 => LatLonPoint.newBoxQuery(field, parts.head.toDouble,parts(2).toDouble,parts(1).toDouble,parts.last.toDouble)
                case count =>
                  val lats = Array[Double](count/2)
                  val lons = Array[Double](count/2)
                  var i = 0
                  var j = 0
                  while (i<count) {
                    lats(j) = parts(i).toDouble
                    i += 1
                    lons(j) = parts(i).toDouble
                    i += 1
                    j += 1
                  }
                  LatLonPoint.newPolygonQuery(field, new Polygon(lats,lons))
              }

            case _ =>
              super.getFieldQuery(field, content, quoted)
          } else {
              val rfield = if (ext == 0) indexMetadata.contentField else field.substring(0,ext)
              val exts = field.substring(ext+1).split("_")
              exts(0) match {
                case "infl" =>
                  val infls = MorphologicalAnalyzer.analyzer.allInflections(content,new Locale(exts(1))).asScala.toSeq
                  exts.length match {
                    case 2 =>
                      new ExtractingTermInSetQuery(rfield,infls.map(indexMetadata.indexingAnalyzers(rfield).normalize(rfield,_)).asJava)
/*                    case 3 if exts(2)=="~1" || exts(2)=="~2" =>
                      val editDistance =  if (exts(2)=="~1") 1 else 2
                      val bqb = new BooleanQuery.Builder()
                      val a = new java.util.ArrayList[Automaton](10000)
                      for (infl <- infls) {
                        a.add(FuzzyTermsEnum.buildAutomaton(infl,0,false,editDistance))
                        if (a.size==10000) {
                          bqb.add(new AutomatonQuery(new Term(rfield,exts(1)),Operations.union(a),Int.MaxValue),Occur.SHOULD)
                          a.clear()
                        }
                      }
                      bqb.add(new AutomatonQuery(new Term(rfield,exts(1)),Operations.union(a),Int.MaxValue),Occur.SHOULD)
                      bqb.build() */
                    case 3 =>
                      val bqb = new BooleanQuery.Builder()
                      val sqp = newQueryParser(level)
                      for (infl <- infls) bqb.add(sqp.parse(indexMetadata.indexingAnalyzers(rfield).normalize(rfield,infl).utf8ToString+exts(2)),Occur.SHOULD)
                      bqb.build()
                    case 4 =>
                      val bqb = new BooleanQuery.Builder()
                      val sqp = newQueryParser(level)
                      for (infl <- infls) bqb.add(sqp.parse(exts(2)+indexMetadata.indexingAnalyzers(rfield).normalize(rfield,infl).utf8ToString+exts(3)),Occur.SHOULD)
                      bqb.build()
                  }
              }
          }
      }

      override def getRangeQuery(field: String, part1: String, part2: String, startInclusive: Boolean, endInclusive: Boolean): Query = {
        level.fields(field).indexedAs match {
          case IndexedFieldType.INTPOINT =>
            val low = if (part1 == null || part1.equals("*")) Int.MinValue else if (startInclusive) part1.toInt else part1.toInt + 1
            val high = if (part2 == null || part2.equals("*")) Int.MaxValue else if (endInclusive) part2.toInt else part2.toInt - 1
            IntPoint.newRangeQuery(field, low, high)
          case IndexedFieldType.FLOATPOINT =>
            val low = if (part1 == null || part1.equals("*")) Float.MinValue else if (startInclusive) part1.toFloat else FloatPoint.nextUp(part1.toFloat)
            val high = if (part2 == null || part2.equals("*")) Float.MaxValue else if (endInclusive) part2.toFloat else FloatPoint.nextDown(part2.toFloat)
            FloatPoint.newRangeQuery(field, low, high)
          case IndexedFieldType.DOUBLEPOINT =>
            val low = if (part1 == null || part1.equals("*")) Double.MinValue else if (startInclusive) part1.toDouble else DoublePoint.nextUp(part1.toDouble)
            val high = if (part2 == null || part2.equals("*")) Double.MaxValue else if (endInclusive) part2.toDouble else DoublePoint.nextDown(part2.toDouble)
            DoublePoint.newRangeQuery(field, low, high)
          case IndexedFieldType.LONGPOINT =>
            val low = if (part1 == null || part1.equals("*")) Long.MinValue else {
              val low = if (part1.contains("-"))
                ISODateTimeFormat.dateOptionalTimeParser().parseMillis(part1)
              else part1.toLong
              if (startInclusive) low else low + 1
            }
            val high = if (part2 == null || part2.equals("*")) Long.MaxValue else {
              val high = if (part2.contains("-"))
                ISODateTimeFormat.dateOptionalTimeParser().parseMillis(part2)
              else part2.toLong
              if (endInclusive) high else high - 1
            }
            LongPoint.newRangeQuery(field, low, high)
          case _ =>
            super.getRangeQuery(field, part1, part2, startInclusive, endInclusive)
        }
      }
    }
    qp.setAllowLeadingWildcard(true)
    qp.setSplitOnWhitespace(false)
    qp.setDefaultOperator(Operator.AND)
    qp.setMaxDeterminizedStates(Int.MaxValue)
    qp
  }

  val termVectorQueryParsers: Map[String,ThreadLocal[QueryParser]] = indexMetadata.levels.map(level => (level.id, new ThreadLocal[QueryParser] {

    override def initialValue(): QueryParser = {
      val qp = newQueryParser(level)
      qp.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE)
      qp
    }

  })).toMap

  val documentQueryParsers: Map[String,ThreadLocal[QueryParser]] = indexMetadata.levels.map(level => (level.id, new ThreadLocal[QueryParser] {

    override def initialValue(): QueryParser = {
      val qp = newQueryParser(level)
      qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE)
      qp
    }

  })).toMap

  private val queryPartStart = "(?<!\\\\)<".r
  private val queryPartEnd = "(?<!\\\\)>".r

  val termsEnums: Map[String,ThreadLocal[TermsEnum]] = indexMetadata.levels.map(level => (level.id, new ThreadLocal[TermsEnum] {

    override def initialValue(): TermsEnum = reader(level.id).leaves.get(0).reader.terms(indexMetadata.contentField).iterator

  })).toMap

  def extractContentTermBytesRefsFromQuery(q: Query): Seq[BytesRef] = QueryTermExtractor.getTerms(q, false, indexMetadata.contentField).map(wt => new BytesRef(wt.getTerm)).toSeq

  class ExtractingTermInSetQuery(field: String, terms: java.util.Collection[BytesRef]) extends TermInSetQuery(field, terms) {

    override def createWeight(searcher: IndexSearcher, scoreMode: ScoreMode, boost: Float): Weight = {
      val weight = super.createWeight(searcher, scoreMode, boost)
      new ConstantScoreWeight(this, boost) {
        override def extractTerms(aterms: java.util.Set[Term]): Unit = for (term <- terms.asScala) aterms.add(new Term(field,term))

        override def scorer(context: LeafReaderContext) = weight.scorer(context)

        override def isCacheable(ctx: LeafReaderContext) = weight.isCacheable(ctx)
      }

    }
  }

  def matchingValuesToQuery(outField: FieldInfo, inField: FieldInfo, is: IndexSearcher, query: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Query = {
    outField.indexedAs match {
      case IndexedFieldType.STRING =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            new ExtractingTermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].asJava)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            new ExtractingTermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].map(v => new BytesRef(v.toString)).asJava)
          case StoredFieldType.FLOATDOCVALUES =>
            new ExtractingTermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].map(v => new BytesRef(v.toString)).asJava)
          case StoredFieldType.DOUBLEDOCVALUES =>
            new ExtractingTermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].map(v => new BytesRef(v.toString)).asJava)
          case _ => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.INTPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS  | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toInt).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].view.map(_.toInt).toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].view.map(_.toInt).toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].view.map(_.toInt).toSeq:_*)
          case _ => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.LONGPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toLong).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].view.map(_.toLong).toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].view.map(_.toLong).toSeq:_*)
          case _ => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.FLOATPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toFloat).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].view.map(_.toFloat).toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].view.map(_.toFloat).toSeq:_*)
          case _ => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.DOUBLEPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDDOCVALUES | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toDouble).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].view.map(_.toDouble).toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].view.map(_.toDouble).toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].toSeq:_*)
          case _ => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.LATLONPOINT =>
        inField.storedAs match {
          case StoredFieldType.LATLONDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].toSeq:_*)
          case _ => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case any => throw new UnsupportedOperationException("Unsupported indexed field type "+any)
    }
  }
  
  private def runSubQuery(queryLevel: String, query: Query, targetLevel: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): Future[Query] = Future {
    if (!indexMetadata.levelOrder.contains(targetLevel)) // targetLevel is not really a target level but a regular field
      matchingValuesToQuery(indexMetadata.levelMap(queryLevel).fields(targetLevel), indexMetadata.levelMap(queryLevel).fields(targetLevel), searcher(queryLevel, QueryScoring.NONE), query)
    else if (indexMetadata.levelOrder(queryLevel)<indexMetadata.levelOrder(targetLevel)) // ql:DOCUMENT < tl:PARAGRAPH
      matchingValuesToQuery(indexMetadata.levelMap(queryLevel).idFieldInfo, indexMetadata.levelMap(queryLevel).idFieldInfo , searcher(targetLevel, QueryScoring.NONE), query)
    else // ql:PARAGRAPH > tl:DOCUMENT
      matchingValuesToQuery(indexMetadata.levelMap(targetLevel).idFieldInfo, indexMetadata.levelMap(targetLevel).idFieldInfo , searcher(queryLevel, QueryScoring.NONE), query)
  }
  
  private def processQueryInternal(exactCounts: Boolean, queryIn: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): (String,Query,String) = {
    val queryLevel = queryIn.substring(1,queryIn.indexOf('§')).toUpperCase
    val targetLevelOrig = queryIn.substring(queryIn.lastIndexOf('§') + 1, queryIn.length - 1)
    val targetLevel = if (indexMetadata.levelOrder.contains(targetLevelOrig.toUpperCase)) targetLevelOrig.toUpperCase else targetLevelOrig
    var query = queryIn.substring(queryIn.indexOf('§') + 1, queryIn.lastIndexOf('§'))
    val replacements = new mutable.HashMap[String,Future[Query]]
    var firstStart = queryPartStart.findFirstMatchIn(query)
    while (firstStart.isDefined) { // we have (more) sub-queries
      val ends = queryPartEnd.findAllMatchIn(query)
      var curEnd = ends.next().start
      val neededEnds = queryPartStart.findAllIn(query.substring(0, curEnd)).size - 1
      while (neededEnds > 0) curEnd = ends.next().start
      val (subQueryQueryLevel, subQuery, subQueryTargetLevel) = processQueryInternal(exactCounts, query.substring(firstStart.get.start, curEnd + 1))
      val processedSubQuery = if (subQueryQueryLevel == queryLevel && subQueryTargetLevel == targetLevel) Future.successful(subQuery) else runSubQuery(subQueryQueryLevel,subQuery,subQueryTargetLevel)
      replacements += (("" + (replacements.size + 1)) -> processedSubQuery)
      query = query.substring(0, firstStart.get.start) + "MAGIC:" + replacements.size + query.substring(curEnd + 1)
      firstStart = queryPartStart.findFirstMatchIn(query)
    }
    logger.debug(s"Query $queryIn rewritten to $query with replacements $replacements.")
    val q = if (query.isEmpty || query == "*") {
      if (exactCounts) new NumericDocValuesWeightedMatchAllDocsQuery(indexMetadata.contentTokensField)
      else new MatchAllDocsQuery()
    } else {
      val q = (if (exactCounts) documentQueryParsers(queryLevel).get else termVectorQueryParsers(queryLevel).get).parse(query) 
      if (exactCounts && q.isInstanceOf[BooleanQuery]) {
        val bq = q.asInstanceOf[BooleanQuery]
        if (bq.clauses.asScala.forall(c => c.getOccur == Occur.MUST_NOT || (c.getQuery.isInstanceOf[BoostQuery] && c.getQuery.asInstanceOf[BoostQuery].getBoost == 0.0f))) {
          val bqb = new BooleanQuery.Builder()
          bqb.add(new NumericDocValuesWeightedMatchAllDocsQuery(indexMetadata.contentTokensField), Occur.MUST)
          bq.clauses.asScala.foreach(bc => { 
            if (bc.getOccur == Occur.MUST) bqb.add(bc.getQuery, Occur.FILTER)
            else bqb.add(bc)
          })
          bqb.build
        } else bq
      } else q match {
        case bq: BoostQuery if bq.getBoost == 0.0 =>
          val bqb = new BooleanQuery.Builder()
          bqb.add(new NumericDocValuesWeightedMatchAllDocsQuery(indexMetadata.contentTokensField), Occur.MUST)
          bqb.add(q, Occur.FILTER)
          bqb.build
        case _ => q
      }
    }
    if (replacements.isEmpty) (queryLevel,q,targetLevel) 
    else q match {
      case bq: BooleanQuery =>
        val bqb = new BooleanQuery.Builder()
        for (clause <- bq.clauses.asScala)
          clause.getQuery match {
            case tq: TermQuery =>
              if (tq.getTerm.field == "MAGIC") bqb.add(Await.result(replacements(tq.getTerm.text), Duration.Inf), clause.getOccur)
              else bqb.add(clause)
            case _ => bqb.add(clause)
          }
        (queryLevel, bqb.build, targetLevel)
      case _ =>
        val q2 = q match {
          case tq: TermQuery =>
            if (tq.getTerm.field == "MAGIC") Await.result(replacements(tq.getTerm.text), Duration.Inf)
            else q
          case _ => q
        }
        (queryLevel, q2, targetLevel)
    }
  }
  
  def buildFinalQueryRunningSubQueries(exactCounts: Boolean, query: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): (String, Query) = {
    val (queryLevel, q, targetLevel) = processQueryInternal(exactCounts, query)
    if (queryLevel==targetLevel) (targetLevel,q)
    else (targetLevel,Await.result(runSubQuery(queryLevel,q,targetLevel), Duration.Inf))
  }
  
}
