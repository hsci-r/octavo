package services

import java.io.{File, FileInputStream}
import java.nio.file.{FileSystems, Path}
import java.util.concurrent.ForkJoinPool
import java.util.{Collections, Locale}
import javax.inject.{Inject, Singleton}

import enumeratum.{Enum, EnumEntry}
import fi.seco.lucene.MorphologicalAnalyzer
import org.apache.lucene.analysis.core.{KeywordAnalyzer, WhitespaceAnalyzer}
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.{Analyzer, CharArraySet}
import org.apache.lucene.document.{DoublePoint, FloatPoint, IntPoint, LongPoint}
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queryparser.classic.QueryParser.Operator
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search._
import org.apache.lucene.search.highlight.QueryTermExtractor
import org.apache.lucene.search.similarities.{BasicStats, SimilarityBase}
import org.apache.lucene.store._
import org.apache.lucene.util.BytesRef
import org.joda.time.format.ISODateTimeFormat
import parameters.SumScaling
import play.api.libs.json.Reads._
import play.api.libs.json._
import play.api.{Configuration, Logger}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.implicitConversions

object IndexAccess {
    
  private val numShortWorkers = sys.runtime.availableProcessors
  private val numLongWorkers = Math.max(sys.runtime.availableProcessors - 2, 1)

  val longTaskForkJoinPool = new ForkJoinPool(numLongWorkers, (pool: ForkJoinPool) => {
    val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
    worker.setName("long-task-worker-" + worker.getPoolIndex)
    worker
  }, null, true)
  
  val longTaskTaskSupport = new ForkJoinTaskSupport(longTaskForkJoinPool)
  val longTaskExecutionContext = ExecutionContext.fromExecutorService(longTaskForkJoinPool)
  
  val shortTaskForkJoinPool = new ForkJoinPool(numShortWorkers, (pool: ForkJoinPool) => {
    val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
    worker.setName("short-task-worker-" + worker.getPoolIndex)
    worker
  }, null, true)
  val shortTaskTaskSupport = new ForkJoinTaskSupport(shortTaskForkJoinPool)
  val shortTaskExecutionContext = ExecutionContext.fromExecutorService(shortTaskForkJoinPool)

  BooleanQuery.setMaxClauseCount(Int.MaxValue)

  private val whitespaceAnalyzer = new WhitespaceAnalyzer()
  
  private val termFrequencySimilarity = new SimilarityBase() {
    override def score(stats: BasicStats, freq: Float, docLen: Float): Float = freq
    override def explain(stats: BasicStats, doc: Int, freq: Explanation, docLen: Float): Explanation = Explanation.`match`(freq.getValue,"")
    override def toString: String = ""
  }
  
  private case class TermsEnumToBytesRefIterator(te: TermsEnum) extends Iterator[BytesRef] {
    var br: BytesRef = te.next()
    var nextFetched: Boolean = true
    override def next(): BytesRef = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      br
    }
    override def hasNext(): Boolean = {
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
    override def next(): (BytesRef,Int) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.docFreq)
      ret
    }
    override def hasNext(): Boolean = {
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
    override def next(): (BytesRef,Long) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.totalTermFreq)
      ret
    }
    override def hasNext(): Boolean = {
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
    override def next(): (BytesRef,Int,Long) = {
      if (!nextFetched) br = te.next()
      else nextFetched = false
      val ret = (br, te.docFreq, te.totalTermFreq)
      ret
    }
    override def hasNext(): Boolean = {
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
    val hc = new TotalHitCountCollector()
    tlc.get.setCollector(hc)
    is.search(q,tlc.get)
    hc.getTotalHits
  }
  
}

@Singleton
class IndexAccessProvider @Inject() (config: Configuration) {
  val indexAccesses = {
    val c = config.get[Configuration]("indices")
    c.keys.par.map(k => (k, new IndexAccess(c.get[String](k)))).seq.toMap
  }
  def apply(id: String): IndexAccess = indexAccesses(id)
  def toJson: JsValue = Json.toJson(indexAccesses.mapValues(ia => ia.indexMetadata.toJson(ia)))
}

sealed abstract class StoredFieldType extends EnumEntry {
  def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue]
  def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_]
}

object StoredFieldType extends Enum[StoredFieldType] {
  case object NUMERICDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
      val dvs = DocValues.getNumeric(lr, field)
      (doc: Int) => if (dvs.advanceExact(doc)) Some(JsNumber(dvs.longValue)) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Long] = {
      val ret = new mutable.HashSet[Long]
      tlc.get.setCollector(new SimpleCollector() {

        override def needsScores: Boolean = false

        var dv: NumericDocValues = _

        override def collect(doc: Int) {
          if (this.dv.advanceExact(doc))
            ret += this.dv.longValue()
        }

        override def doSetNextReader(context: LeafReaderContext) {
          this.dv = DocValues.getNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"NumericDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object FLOATDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
      val dvs = DocValues.getNumeric(lr, field)
      (doc: Int) => if (dvs.advanceExact(doc)) {
        Some(JsNumber(BigDecimal(java.lang.Float.intBitsToFloat(dvs.longValue.toInt).toDouble))) } else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Float] = {
      val ret = new mutable.HashSet[Float]
      tlc.get.setCollector(new SimpleCollector() {

        override def needsScores: Boolean = false

        var dv: NumericDocValues = _

        override def collect(doc: Int) {
          if (this.dv.advanceExact(doc))
            ret += java.lang.Float.intBitsToFloat(this.dv.longValue.toInt)
        }

        override def doSetNextReader(context: LeafReaderContext) {
          this.dv = DocValues.getNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"FloatDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object DOUBLEDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
      val dvs = DocValues.getNumeric(lr, field)
      (doc: Int) => if (dvs.advanceExact(doc)) Some(JsNumber(BigDecimal(java.lang.Double.longBitsToDouble(dvs.longValue)))) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[Double] = {
      val ret = new mutable.HashSet[Double]
      tlc.get.setCollector(new SimpleCollector() {

        override def needsScores: Boolean = false

        var dv: NumericDocValues = _

        override def collect(doc: Int) {
          if (this.dv.advanceExact(doc))
            ret += java.lang.Double.longBitsToDouble(this.dv.longValue)
        }

        override def doSetNextReader(context: LeafReaderContext) {
          this.dv = DocValues.getNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"DoubleDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }
  case object SORTEDDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
      val dvs = DocValues.getSorted(lr, field)
      if (containsJson)
        (doc: Int) => if (dvs.advanceExact(doc)) Some(Json.parse(dvs.binaryValue.utf8ToString())) else None
      else
        (doc: Int) => if (dvs.advanceExact(doc)) Some(JsString(dvs.binaryValue.utf8ToString())) else None
    }
    def getMatchingValues(is: IndexSearcher, q: Query, field: String)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[BytesRef] = {
      val ret = new mutable.HashSet[BytesRef]
      tlc.get.setCollector(new SimpleCollector() {
        
        override def needsScores: Boolean = false
        
        var dv: SortedDocValues = _
  
        override def collect(doc: Int) {
          if (this.dv.advanceExact(doc))
            ret += BytesRef.deepCopyOf(this.dv.binaryValue())
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          this.dv = DocValues.getSorted(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"SortedDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }
  }    
  case object SORTEDNUMERICDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
      val dvs = DocValues.getSortedNumeric(lr, field)
      (doc: Int) => if (dvs.advanceExact(doc)) Some({
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
        
        override def needsScores: Boolean = false
        
        var dv: SortedNumericDocValues = _
  
        override def collect(doc: Int) {
          if (this.dv.advanceExact(doc)) {
            var i = 0
            while (i<values.length) {
              ret += this.dv.nextValue()
              i += 1
            }
          }
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          this.dv = DocValues.getSortedNumeric(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"SortedNumericDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }        
  }
  case object SORTEDSETDOCVALUES extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
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
        
        override def needsScores: Boolean = false
        
        var dv: SortedSetDocValues = _
  
        override def collect(doc: Int) {
          if (this.dv.advanceExact(doc)) {
            var ord = this.dv.nextOrd
            while (ord != SortedSetDocValues.NO_MORE_ORDS) {
              ret += BytesRef.deepCopyOf(this.dv.lookupOrd(ord))
              ord = this.dv.nextOrd
            }
          }
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          this.dv = DocValues.getSortedSet(context.reader, field)
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"SortedSetDocValues -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object SINGULARSTOREDFIELD extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
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
        
        override def needsScores: Boolean = false
        
        var r: LeafReader = _
  
        override def collect(doc: Int) {
          val fv = r.document(doc, fieldS).getBinaryValue(field)
          if (fv != null) ret += BytesRef.deepCopyOf(fv)
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          this.r = context.reader
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"SingularStoredField -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object MULTIPLESTOREDFIELDS extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = {
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
        
        override def needsScores: Boolean = false
        
        var r: LeafReader = _
  
        override def collect(doc: Int) {
          for (fv <- r.document(doc, fieldS).getBinaryValues(field)) ret += BytesRef.deepCopyOf(fv)
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          this.r = context.reader
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"MultiStoredField -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object TERMVECTOR extends StoredFieldType {
    import IndexAccess._
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] =
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
        
        override def needsScores: Boolean = false
        
        var r: LeafReader = _
  
        override def collect(doc: Int) {
          r.getTermVector(doc, field).asBytesRefIterable.foreach(ret += BytesRef.deepCopyOf(_))
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          this.r = context.reader
        }
      })
      is.search(q, tlc.get)
      Logger.debug(f"Termvector -subquery on $field%s: $q%s returning ${ret.size}%,d hits")
      ret
    }    
  }
  case object NONE extends StoredFieldType {
    def jsGetter(lr: LeafReader, field: String, containsJson: Boolean): (Int) => Option[JsValue] = throw new IllegalArgumentException
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
  case object NONE extends IndexedFieldType
  val values = findValues
}

case class FieldInfo(
  id: String,
  description: String,
  storedAs: StoredFieldType,
  indexedAs: IndexedFieldType,
  containsJson: Boolean
) {
  def getMatchingValues(is: IndexSearcher, q: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_] = storedAs.getMatchingValues(is, q, id)
  def jsGetter(lr: LeafReader): (Int) => Option[JsValue] = storedAs.jsGetter(lr, id, containsJson)
  def toJson(lr: LeafReader) = {
    val ret = Json.obj(
      "description"->description,
      "storedAs"->storedAs.toString,
      "indexedAs"->indexedAs.toString,
      "containsJson"->containsJson
    )
    if ((indexedAs == IndexedFieldType.TEXT || indexedAs == IndexedFieldType.STRING)) ret ++ Json.obj("distinctValues"-> (if (lr.terms(id) != null) lr.terms(id).size() else 0))
    else ret
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
  def toJson(ia: IndexAccess, commonFields: Set[FieldInfo]): JsValue = Json.obj(
    "id"->id,
    "description"->description,
    "idField"->idField,
    "indices"->indices,
    "preload"->preload,
    "fields"->fields.filter(p => !commonFields.contains(p._2)).mapValues(_.toJson(ia.reader(id).leaves.get(0).reader))
  )
  def getMatchingValues(is: IndexSearcher, q: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Set[_] = fields(idField).getMatchingValues(is, q)
}

case class IndexMetadata(
  indexName: String,
  indexVersion: String,
  indexType: String,
  contentField: String,
  contentTokensField: String,
  levels: Seq[LevelMetadata],
  defaultLevelS: Option[String],
  indexingAnalyzersAsText: Map[String,String]
) {
  
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
  val indexingAnalyzers: Map[String,Analyzer] = indexingAnalyzersAsText.mapValues{
    case "StandardAnalyzer" => new StandardAnalyzer(CharArraySet.EMPTY_SET)
    case a if a.startsWith("MorphologicalAnalyzer_") => new MorphologicalAnalyzer(new Locale(a.substring(23)))
    case any => throw new IllegalArgumentException("Unknown analyzer type " + any)
  }.withDefaultValue(new KeywordAnalyzer())
  val levelOrder: Map[String,Int] = levels.map(_.id).zipWithIndex.toMap
  val levelMap: Map[String,LevelMetadata] = levels.map(l => (l.id,l)).toMap
  val defaultLevel: LevelMetadata = defaultLevelS.map(levelMap(_)).getOrElse(levels.last)
  val commonFields: Map[String, FieldInfo] = levels.map(_.fields).reduce((l, r) => l.filter(lp => r.get(lp._1).contains(lp._2)))
  def toJson(ia: IndexAccess) = Json.obj(
    "name"->indexName,
    "version"->indexVersion,
    "indexType"->indexType,
    "contentField"->contentField,
    "contentTokensField"->contentTokensField,
    "levels"->levels.map(_.toJson(ia,commonFields.values.toSet)),
    "defaultLevel"->defaultLevel.id,
    "indexingAnalyzers"->indexingAnalyzersAsText,
    "commonFields"->commonFields.mapValues(_.toJson(ia.reader(defaultLevel.id).leaves.get(0).reader))
  )
}

class IndexAccess(path: String) {
  
  import IndexAccess._
 
  private val readers = new mutable.HashMap[String,IndexReader]
  private val tfSearchers = new mutable.HashMap[String,IndexSearcher]
  private val tfidfSearchers = new mutable.HashMap[String,IndexSearcher]
  
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
  
  def readIndexMetadata(c: JsValue) = IndexMetadata(
    (c \ "name").as[String],
    (c \ "version").as[String],
    (c \ "indexType").asOpt[String].getOrElse("MMapDirectory"),
    (c \ "contentField").as[String],
    (c \ "contentTokensField").as[String],
    (c \ "levels").as[Seq[JsValue]].map(readLevelMetadata(_,
      (c \ "commonFields").asOpt[Map[String,JsValue]].map(readFieldInfos).getOrElse(Map.empty)
    )),
    (c \ "defaultLevel").asOpt[String],
    (c \ "indexingAnalyzers").asOpt[Map[String,String]].getOrElse(Map.empty)
  )

  val indexMetadata: IndexMetadata = try {
    readIndexMetadata(Json.parse(new FileInputStream(new File(path+"/indexmeta.json"))))
  } catch {
    case e: Exception =>
      Logger.error("Encountered an exception reading "+path+"/indexmeta.json",e)
      throw e
  }

  try {
    val futures = for (level <- indexMetadata.levels) yield Future {
      Logger.info("Initializing index at "+path+"/["+level.indices.mkString(", ")+"]")
      val seenFields = new collection.mutable.HashSet[String]()
      val mreaders = level.indices.map(index => {
        val directory = indexMetadata.directoryCreator(FileSystems.getDefault.getPath(path+"/"+index))
        if (level.preload && directory.isInstanceOf[MMapDirectory]) directory.asInstanceOf[MMapDirectory].setPreload(true)
        val ret = DirectoryReader.open(directory)
        for (fi <- ret.leaves().get(0).reader().getFieldInfos.iterator.asScala) {
          fi.getDocValuesType
          if (!level.fields.contains(fi.name)) Logger.warn(s"Encountered undocumented field ${fi.name} in ${path + "/" + index}. Won't know how to handle/publicise it.")
          else seenFields += fi.name
        }
        ret
      })
      val reader = if (mreaders.length == 1) mreaders.head else new ParallelCompositeReader(mreaders:_*)
      for (field <- level.fields) if (!seenFields.contains(field._1)) Logger.warn("Documented field "+ field._1 + " not found in "+path+"/["+level.indices.mkString(", ")+"]")
      readers.put(level.id, reader)
      val tfSearcher = new IndexSearcher(reader)
      tfSearcher.setSimilarity(termFrequencySimilarity)
      tfSearchers.put(level.id, tfSearcher)
      tfidfSearchers.put(level.id, new IndexSearcher(reader))
      Logger.info("Initialized index at "+path+"/["+level.indices.mkString(", ")+"]")
      (level.id, reader)
    }(shortTaskExecutionContext)
    futures.foreach(Await.result(_, Duration.Inf))
  } catch {
    case e: Exception =>
      Logger.error("Encountered an exception initializing index at "+path,e)
      throw e
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
  
  val queryAnalyzers = indexMetadata.levels.map(level => (level.id, new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),
      level.fields.values.filter(_.indexedAs == IndexedFieldType.TEXT).map(_.id).map((_,whitespaceAnalyzer)).toMap[String,Analyzer].asJava))).toMap
  
  private def newQueryParser(level: LevelMetadata) = {
    val qp = new QueryParser(indexMetadata.contentField,queryAnalyzers(level.id)) {
      override def getFieldQuery(field: String, content: String, quoted: Boolean): Query = {
        level.fields(field).indexedAs match {
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
          case _ =>
            super.getFieldQuery(field, content, quoted)
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
  
  def docFreq(ir: IndexReader, term: Long): Int = {
    val it = ir.leaves.get(0).reader.terms(indexMetadata.contentField).iterator
    it.seekExact(term)
    it.docFreq
  }

  def totalTermFreq(ir: IndexReader, term: Long): Long = {
    val it = ir.leaves.get(0).reader.terms(indexMetadata.contentField).iterator
    it.seekExact(term)
    it.totalTermFreq
  }

  def termOrdToTerm(ir: IndexReader, term: Long): String = {
    val it = ir.leaves.get(0).reader.terms(indexMetadata.contentField).iterator
    it.seekExact(term)
    it.term.utf8ToString
  }
  
  def extractContentTermsFromQuery(q: Query): Seq[String] = QueryTermExtractor.getTerms(q, false, indexMetadata.contentField).map(_.getTerm).toSeq

  def matchingValuesToQuery(outField: FieldInfo, inField: FieldInfo, is: IndexSearcher, query: Query)(implicit tlc: ThreadLocal[TimeLimitingCollector]): Query = {
    outField.indexedAs match {
      case IndexedFieldType.STRING =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            new TermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].asJava)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            new TermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].map(v => new BytesRef(v.toString)).asJava)
          case StoredFieldType.FLOATDOCVALUES =>
            new TermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].map(v => new BytesRef(v.toString)).asJava)
          case StoredFieldType.DOUBLEDOCVALUES =>
            new TermInSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].map(v => new BytesRef(v.toString)).asJava)
          case any => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.INTPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toInt).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].view.map(_.toInt).toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].view.map(_.toInt).toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            IntPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].view.map(_.toInt).toSeq:_*)
          case any => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.LONGPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toLong).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].view.map(_.toLong).toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            LongPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].view.map(_.toLong).toSeq:_*)
          case any => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.FLOATPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toFloat).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].view.map(_.toFloat).toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            FloatPoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].view.map(_.toFloat).toSeq:_*)
          case any => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case IndexedFieldType.DOUBLEPOINT =>
        inField.storedAs match {
          case StoredFieldType.SINGULARSTOREDFIELD | StoredFieldType.MULTIPLESTOREDFIELDS | StoredFieldType.SORTEDSETDOCVALUES | StoredFieldType.TERMVECTOR =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[BytesRef]].view.map(_.utf8ToString.toDouble).toSeq:_*)
          case StoredFieldType.NUMERICDOCVALUES | StoredFieldType.SORTEDNUMERICDOCVALUES =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Long]].view.map(_.toDouble).toSeq:_*)
          case StoredFieldType.FLOATDOCVALUES =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Float]].view.map(_.toDouble).toSeq:_*)
          case StoredFieldType.DOUBLEDOCVALUES =>
            DoublePoint.newSetQuery(outField.id, inField.getMatchingValues(is, query).asInstanceOf[scala.collection.Set[Double]].toSeq:_*)
          case any => throw new UnsupportedOperationException("Unsupported field type combo "+inField+" => "+outField)
        }
      case any => throw new UnsupportedOperationException("Unsupported indexed field type "+any)
    }
  }
  
  private def runSubQuery(queryLevel: String, query: Query, targetLevel: String)(implicit iec: ExecutionContext, tlc: ThreadLocal[TimeLimitingCollector]): Future[Query] = Future {
    if (!indexMetadata.levelOrder.contains(targetLevel)) // targetLevel is not really a target level but a regular field
      matchingValuesToQuery(indexMetadata.levelMap(queryLevel).fields(targetLevel), indexMetadata.levelMap(queryLevel).fields(targetLevel), searcher(queryLevel, SumScaling.ABSOLUTE), query)
    else if (indexMetadata.levelOrder(queryLevel)<indexMetadata.levelOrder(targetLevel)) // ql:DOCUMENT < tl:PARAGRAPH
      matchingValuesToQuery(indexMetadata.levelMap(queryLevel).idFieldInfo, indexMetadata.levelMap(queryLevel).idFieldInfo , searcher(targetLevel, SumScaling.ABSOLUTE), query)
    else // ql:PARAGRAPH > tl:DOCUMENT
      matchingValuesToQuery(indexMetadata.levelMap(targetLevel).idFieldInfo, indexMetadata.levelMap(targetLevel).idFieldInfo , searcher(queryLevel, SumScaling.ABSOLUTE), query)
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
    Logger.debug(s"Query $queryIn rewritten to $query with replacements $replacements.")
    val q = if (query.isEmpty) {
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
