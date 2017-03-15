package services

import org.apache.lucene.search.Query
import parameters.LocalTermVectorProcessingParameters
import parameters.QueryReturnParameters
import org.apache.lucene.search.TimeLimitingCollector
import scala.collection.mutable.PriorityQueue
import com.koloboke.function.LongDoubleConsumer
import parameters.AggregateTermVectorProcessingParameters
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.index.LeafReaderContext
import com.koloboke.collect.map.LongIntMap
import com.koloboke.collect.map.hash.HashLongDoubleMaps
import com.koloboke.function.LongIntConsumer
import com.koloboke.collect.map.LongDoubleMap
import org.apache.lucene.index.IndexReader
import play.api.Logger
import parameters.LocalTermVectorScaling
import org.apache.lucene.codecs.compressing.OrdTermVectorsReader.TVTermsEnum
import org.apache.lucene.search.SimpleCollector
import com.koloboke.collect.map.hash.HashLongIntMaps
import javax.inject.Singleton
import javax.inject.Inject
import org.apache.lucene.util.BytesRef
import mdsj.MDSJ
import scala.collection.mutable.HashMap
import com.koloboke.collect.set.LongSet
import java.util.function.LongConsumer
import com.koloboke.collect.set.hash.HashLongSets
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import scala.collection.mutable.ArrayBuffer

object TermVectors  {
  
  import IndexAccess.{getHitCountForQuery,termOrdToTerm}
  
  case class TermVectorQueryMetadata(val totalDocs: Long, val processedDocs: Long, val samplePercentage: Double, val contributingDocs: Long, val processedTerms: Long, val acceptedTerms: Long, val totalAcceptedTermFreq: Long) {
    def toJson(): JsValue = Json.toJson(Map(
        "totalDocsMatchingQuery"->Json.toJson(totalDocs),
        "processedDocs"->Json.toJson(processedDocs),
        "samplePercentage"->Json.toJson(samplePercentage),
        "contributingDocs"->Json.toJson(contributingDocs),
        "processedTerms"->Json.toJson(processedTerms),
        "acceptedTerms"->Json.toJson(acceptedTerms),
        "totalAcceptedTermFreq"->Json.toJson(totalAcceptedTermFreq)
    ))
            
  }
  
  private def runTermVectorQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], maxDocs: Int, contextSetter: (LeafReaderContext) => Unit, docCollector: (Int) => Unit, termCollector: (Long,Int) => Unit)(implicit tlc: ThreadLocal[TimeLimitingCollector]): TermVectorQueryMetadata = {
   var processedDocs = 0l
   var contributingDocs = 0l
   var processedTerms = 0l
   var acceptedTerms = 0l
   var totalAcceptedTermFreq = 0l
   val totalHits = getHitCountForQuery(is, q)
   val sampleProbability = if (maxDocs == -1) 1.0 else math.min(maxDocs.toDouble / totalHits, 1.0)
   val ir = is.getIndexReader
   Logger.info(f"q: $q%s, sampleProbability:${sampleProbability}%,.4f <- maxDocs:$maxDocs%,d, hits:${totalHits}%,d")
   tlc.get.setCollector(new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        if (maxDocs == -1 || sampleProbability == 1.0 || Math.random() < sampleProbability) {
          processedDocs+=1          
          val tv = this.context.reader.getTermVector(doc, "content")
          if (tv != null) {
            docCollector(doc)
            if (tv.size()>10000) Logger.debug(f"Long term vector for doc $doc%d: ${tv.size}%,d")
            val tvt = tv.iterator().asInstanceOf[TVTermsEnum]
            val min = if (ctvp.localScaling!=LocalTermVectorScaling.MIN) 0 else if (minScalingTerms.isEmpty) Int.MaxValue else minScalingTerms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
            var term = tvt.nextOrd()
            var anyMatches = false
            while (term != -1l) {
              processedTerms += 1
              if (ctvp.matches(ir, term, tvt.totalTermFreq)) {
                acceptedTerms += 1
                anyMatches = true
                val d = ctvp.localScaling match {
                  case LocalTermVectorScaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                  case LocalTermVectorScaling.ABSOLUTE => tvt.totalTermFreq.toInt
                  case LocalTermVectorScaling.FLAT => 1
                }
                termCollector(term, d)
                totalAcceptedTermFreq += d
              }
              term = tvt.nextOrd()
            }
            if (anyMatches) contributingDocs+=1
          }
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
        contextSetter(context)
      }
    })
    is.search(q, tlc.get)
    Logger.info(f"$q%s, total docs: $totalHits%,d, processed docs: $processedDocs%,d, sample probability: $sampleProbability%,.2f. Contributing docs: $contributingDocs%,d, processed terms: $processedTerms%,d, accepted terms: $acceptedTerms%,d, total accepted term freq: $totalAcceptedTermFreq%,d")
    TermVectorQueryMetadata(totalHits, processedDocs, sampleProbability, contributingDocs, processedTerms, acceptedTerms, totalAcceptedTermFreq)
  }
  
  def getTermVectorForDocument(ir: IndexReader, doc: Int, ctvpl: LocalTermVectorProcessingParameters, ctvpa: AggregateTermVectorProcessingParameters): LongDoubleMap = {
    val cv = HashLongIntMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap() 
    val tv = ir.getTermVector(doc, "content")
    if (tv != null) {
      val tvt = tv.iterator.asInstanceOf[TVTermsEnum]
      var term = tvt.nextOrd()
      while (term != -1l) {
        if (ctvpl.matches(ir, term, tvt.totalTermFreq))
          cv.addValue(term, tvt.totalTermFreq.toInt)
        term = tvt.nextOrd()
      }
    }
    scaleAndFilterTermVector(ir, cv, ctvpa)
  }

  private def getUnscaledAggregateContextVectorForQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (TermVectorQueryMetadata,LongIntMap) = {
     val cv = HashLongIntMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()     val md = runTermVectorQuery(is, q, ctvp, minScalingTerms, maxDocs, (_: LeafReaderContext) => Unit, (_: Int) => Unit, (term: Long, freq: Int) => cv.addValue(term, freq))
     (md,cv)
  }
  
  private def scaleAndFilterTermVector(ir: IndexReader, cv: LongIntMap, ctvp: AggregateTermVectorProcessingParameters): LongDoubleMap = {
    val m = HashLongDoubleMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()
    cv.forEach(new LongIntConsumer {
       override def accept(k: Long, v: Int) {
         if (ctvp.matches(v)) m.put(k, ctvp.sumScaling(ir, k, v))
       }
    })
    if (ctvp.limit == -1)
      m
    else {
      val best = filterHighestScores(m, ctvp.limit)
      val m2 = HashLongDoubleMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()
      for ((key,value) <- best) m2.put(key, value)
      m2
    }
  }
  
  def getAggregateContextVectorForQuery(is: IndexSearcher, q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (TermVectorQueryMetadata,LongDoubleMap) = {
    val (md, cv) = getUnscaledAggregateContextVectorForQuery(is, q, ctvpl, minScalingTerms, maxDocs)
    (md, scaleAndFilterTermVector(is.getIndexReader, cv, ctvpa))
  }
  
  private final class UnscaledVectorInfo {
    var docFreq = 0l
    var totalTermFreq = 0l
    val cv: LongIntMap = HashLongIntMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()
  }
  
  private def getGroupedUnscaledAggregateContextVectorsForQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], attr: String, attrLength: Int, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Map[String,UnscaledVectorInfo] = {
    val cvm = new HashMap[String,UnscaledVectorInfo]
    var cv: UnscaledVectorInfo = null
    var attrGetter: (Int) => String = null
    var anyMatches = false
    runTermVectorQuery(is, q, ctvp, minScalingTerms, maxDocs, (nlrc: LeafReaderContext) => {
      attrGetter = QueryReturnParameters.getter(nlrc.reader, attr).andThen(_.iterator.next)
    }, (doc: Int) => {
      if (anyMatches) cv.docFreq += 1
      val cattr = attrGetter(doc)
      cv = cvm.getOrElseUpdate(if (attrLength == -1) cattr else cattr.substring(0,attrLength), new UnscaledVectorInfo)
      anyMatches = false
    }, (term: Long, freq: Int) => {
        anyMatches = true
       cv.cv.addValue(term, freq)
       cv.totalTermFreq += freq
     })
    cvm
  }  

  final class VectorInfo(ir: IndexReader, value: UnscaledVectorInfo, ctvpa: AggregateTermVectorProcessingParameters) {
    val docFreq = value.docFreq
    val totalTermFreq = value.totalTermFreq
    val cv: LongDoubleMap = scaleAndFilterTermVector(ir, value.cv,ctvpa)
  }

  def getGroupedAggregateContextVectorsForQuery(is: IndexSearcher, q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], attr: String, attrLength: Int, ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Map[String,VectorInfo] = {
    val ir = is.getIndexReader()
    getGroupedUnscaledAggregateContextVectorsForQuery(is, q, ctvpl, minScalingTerms, attr, attrLength, maxDocs).map{ case (key,value) => (key, new VectorInfo(ir, value,ctvpa)) }
  }
  
  def getContextTermsForQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (Long,Long,LongSet) = {
     val cv = HashLongSets.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableSet()
     val (docFreq, totalTermFreq) = runTermVectorQuery(is, q, ctvp, Seq.empty, maxDocs, (_: LeafReaderContext) => Unit, (_: Int) => Unit, (term: Long, _) => cv.add(term))
     (docFreq,totalTermFreq,cv)
  }  
  
  def filterHighestScores(cv: LongDoubleMap, limit: Int): Map[Long,Double] = {
    val maxHeap = PriorityQueue.empty[(Long,Double)](new Ordering[(Long,Double)] {
      override def compare(x: (Long,Double), y: (Long,Double)) = y._2 compare x._2
    })
    var total = 0
    cv.forEach(new LongDoubleConsumer {
      override def accept(term: Long, score: Double) {
        total+=1
        if (limit == -1 || total<=limit) 
          maxHeap += ((term,score))
        else if (maxHeap.head._2<score) {
          maxHeap.dequeue()
          maxHeap += ((term,score))
        }
      }      
    })
    maxHeap.toMap
  }
  
  def mds(termVectors: Iterable[LongDoubleMap], rtp: AggregateTermVectorProcessingParameters): Array[Array[Double]] = {
    val tvms = termVectors.toSeq
    val matrix = new Array[Array[Double]](tvms.size)
    for (i <- 0 until matrix.length)
      matrix(i) = new Array[Double](matrix.length)
    for (i <- 0 until matrix.length) {
      for (j <- i + 1 until matrix.length) {
        val dis = rtp.distance(tvms(i), tvms(j))
        matrix(i)(j) = dis
        matrix(j)(i) = dis
      }
    }
    return MDSJ.stressMinimization(matrix, rtp.mdsDimensions).transpose
  }
  
  def termOrdMapToTermMap(ir: IndexReader, m: LongDoubleMap): collection.Map[String,Double] = {
    val rm = new HashMap[String,Double]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm.put(termOrdToTerm(ir, term),freq)
      }
    })
    rm
  }
  
  def termOrdMapToOrderedTermSeq(ir: IndexReader, m: LongDoubleMap): Seq[(String,Double)] = {
    val rm = new ArrayBuffer[(String,Double)]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm += ((termOrdToTerm(ir, term),freq))
      }
    })
    rm.sortBy(-_._2)
  }
  
  def termOrdsToTerms(ir: IndexReader, m: LongSet): Traversable[String] = {
    return new Traversable[String] {
      override def foreach[U](f: String => U): Unit = {
        m.forEach(new LongConsumer {
          override def accept(term: Long) {
            f(termOrdToTerm(ir, term))
          }
        })
      }
    }
  }  
  

}
