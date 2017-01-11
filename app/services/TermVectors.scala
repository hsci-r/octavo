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

object TermVectors  {
  
  import IndexAccess.{getHitCountForQuery,termOrdToTerm}
  
  private def runTermVectorQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], maxDocs: Int, contextSetter: (LeafReaderContext) => Unit, docCollector: (Int) => Unit, termCollector: (Long,Int) => Unit)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (Long,Long) = {
   var aDocFreq = 0l
   var docFreq = 0l
   var t2 = 0l
   var totalTermFreq = 0l
   val totalHits = getHitCountForQuery(is, q)
   val sampleProbability = math.min(maxDocs.toDouble / totalHits, 1.0)
   val ir = is.getIndexReader
   Logger.info(f"q: $q%s, sampleProbability:${sampleProbability}%,.4f <- maxDocs:$maxDocs%,d, hits:${totalHits}")
   tlc.get.setCollector(new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        aDocFreq+=1
        if (maxDocs == -1 || sampleProbability == 1.0 || Math.random() < sampleProbability) {
          docCollector(doc)
          val tv = this.context.reader.getTermVector(doc, "content")
          if (tv.size()>10000) Logger.debug(s"Long term vector for doc $doc: ${tv.size}")
          val tvt = tv.iterator().asInstanceOf[TVTermsEnum]
          val min = if (ctvp.localScaling!=LocalTermVectorScaling.MIN) 0 else if (minScalingTerms.isEmpty) Int.MaxValue else minScalingTerms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
          var term = tvt.nextOrd()
          var anyMatches = false
          while (term != -1l) {
            t2+=1
            if (ctvp.matches(ir, term, tvt.totalTermFreq)) {
              anyMatches = true
              val d = ctvp.localScaling match {
                case LocalTermVectorScaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                case LocalTermVectorScaling.ABSOLUTE => tvt.totalTermFreq.toInt
                case LocalTermVectorScaling.FLAT => 1
              }
              termCollector(term, d)
              totalTermFreq += d
            }
            term = tvt.nextOrd()
          }
          if (anyMatches) docFreq+=1
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
        contextSetter(context)
      }
    })
    is.search(q, tlc.get)
    Logger.info(f"$q%s, processed docs: $aDocFreq%,d, contributing docs: $docFreq%,d, processed terms: $t2%,d, total accepted term freq: $totalTermFreq%,d")
    (docFreq, totalTermFreq)
  }
  
  def getTermVectorForDocument(ir: IndexReader, doc: Int, ctvpl: LocalTermVectorProcessingParameters, ctvpa: AggregateTermVectorProcessingParameters): LongDoubleMap = {
    val cv = HashLongIntMaps.newUpdatableMap() 
    val tvt = ir.getTermVector(doc, "content").iterator.asInstanceOf[TVTermsEnum]
    var term = tvt.nextOrd()
    while (term != -1l) {
      if (ctvpl.matches(ir, term, tvt.totalTermFreq))
        cv.addValue(term, tvt.totalTermFreq.toInt)
      term = tvt.nextOrd()
    }
    scaleAndFilterTermVector(ir, cv, ctvpa)
  }

  private def getUnscaledAggregateContextVectorForQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (Long,Long,LongIntMap) = {
     val cv = HashLongIntMaps.newUpdatableMap()
     val (docFreq, totalTermFreq) = runTermVectorQuery(is, q, ctvp, minScalingTerms, maxDocs, (_: LeafReaderContext) => Unit, (_: Int) => Unit, (term: Long, freq: Int) => cv.put(term, freq))
     (docFreq,totalTermFreq,cv)
  }
  
  private def scaleAndFilterTermVector(ir: IndexReader, cv: LongIntMap, ctvp: AggregateTermVectorProcessingParameters): LongDoubleMap = {
    val m = HashLongDoubleMaps.newUpdatableMap()
    cv.forEach(new LongIntConsumer {
       override def accept(k: Long, v: Int) {
         if (ctvp.matches(v)) m.put(k, ctvp.sumScaling(ir, k, v))
       }
    })
    m
  }
  
  def getAggregateContextVectorForQuery(is: IndexSearcher, q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): (Long,Long,LongDoubleMap) = {
    val (docFreq, totalTermFreq, cv) = getUnscaledAggregateContextVectorForQuery(is, q, ctvpl, minScalingTerms, maxDocs)
    (docFreq, totalTermFreq, scaleAndFilterTermVector(is.getIndexReader, cv, ctvpa))
  }
  
  private final class UnscaledVectorInfo {
    var docFreq = 0l
    var totalTermFreq = 0l
    val cv: LongIntMap = HashLongIntMaps.getDefaultFactory.newUpdatableMap()
  }
  
  private def getGroupedUnscaledAggregateContextVectorsForQuery(is: IndexSearcher, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], attr: String, attrLength: Int, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector]): collection.Map[String,UnscaledVectorInfo] = {
    val cvm = new HashMap[String,UnscaledVectorInfo]
    var cv: UnscaledVectorInfo = null
    var attrGetter: (Int) => String = null
    runTermVectorQuery(is, q, ctvp, minScalingTerms, maxDocs, (nlrc: LeafReaderContext) => {
      attrGetter = QueryReturnParameters.getter(nlrc.reader, attr).andThen(_.iterator.next)
    }, (doc: Int) => {
      val cattr = attrGetter(doc)
      cv = cvm.getOrElseUpdate(if (attrLength == -1) cattr else cattr.substring(0,attrLength), new UnscaledVectorInfo)
    }, (term: Long, freq: Int) => {
       cv.cv.put(term, freq)
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
     val cv = HashLongSets.newUpdatableSet()
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