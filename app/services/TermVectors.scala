package services

import java.util.concurrent.{ExecutorService, ForkJoinPool}
import java.util.function.LongConsumer

import com.jujutsu.tsne.barneshut.BHTSne
import com.jujutsu.utils.TSneUtils
import com.koloboke.collect.map.hash.{HashLongDoubleMaps, HashLongIntMaps}
import com.koloboke.collect.map.{LongDoubleMap, LongIntMap}
import com.koloboke.collect.set.LongSet
import com.koloboke.collect.set.hash.HashLongSets
import com.koloboke.function.{LongDoubleConsumer, LongIntConsumer}
import mdsj.MDSJ
import org.apache.lucene.codecs.compressing.OrdTermVectorsReader.TVTermsEnum
import org.apache.lucene.index.{IndexReader, LeafReaderContext, TermsEnum}
import org.apache.lucene.search.{IndexSearcher, Query, ScoreMode, SimpleCollector, TimeLimitingCollector}
import org.apache.lucene.util.BytesRef
import parameters._
import play.api.Logging
import play.api.libs.json.{JsString, JsValue, Json}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ParIterable, ParSeq, TaskSupport}

object TermVectors extends Logging {
  
  import IndexAccess._
  
  def toParallel[T](i: Seq[T])(implicit ts: TaskSupport): ParSeq[T] = {
    val par = i.par
    par.tasksupport = ts
    par
  }

  def toParallel[T](i: Traversable[T])(implicit ts: TaskSupport): ParIterable[T] = {
    val par = i.par
    par.tasksupport = ts
    par
  }

  case class TermVectorQueryMetadata(totalDocs: Long, processedDocs: Long, samplePercentage: Double, contributingDocs: Long, processedTerms: Long, acceptedTerms: Long, totalAcceptedTermFreq: Long) {
    def toJson = Json.obj(
        "totalDocsMatchingQuery"->totalDocs,
        "processedDocs"->processedDocs,
        "sample"->samplePercentage,
        "contributingDocs"->contributingDocs,
        "processedTerms"->processedTerms,
        "acceptedTerms"->acceptedTerms,
        "totalAcceptedTermFreq"->totalAcceptedTermFreq)

    def combine(other: TermVectorQueryMetadata): TermVectorQueryMetadata = TermVectorQueryMetadata(this.totalDocs+other.totalDocs,this.processedDocs+other.processedDocs,(this.samplePercentage+other.samplePercentage)/2,this.contributingDocs+other.contributingDocs,this.processedTerms+other.processedTerms,this.acceptedTerms+other.acceptedTerms,this.totalAcceptedTermFreq+other.totalAcceptedTermFreq)

  }
  
  private def runTermVectorQuery(
      is: IndexSearcher,
      it: TermsEnum,
      q: Query, 
      ctvp: LocalTermVectorProcessingParameters, 
      minScalingTerms: Seq[BytesRef],
      maxDocs: Int,
      contextSetter: (LeafReaderContext) => Unit, 
      docCollector: (Int) => Unit, 
      termCollector: (Long,Int) => Unit)(implicit tlc: ThreadLocal[TimeLimitingCollector], ia: IndexAccess, qm: QueryMetadata): TermVectorQueryMetadata = {
    var processedDocs = 0l
    var contributingDocs = 0l
    var processedTerms = 0l
    var acceptedTerms = 0l
    var totalAcceptedTermFreq = 0l
    val termTransformer = ctvp.termTransformer.map(_.get)
    termTransformer.foreach(_.getBinding.invokeMethod("setIndexAccess", ia))
    val totalHits = getHitCountForQuery(is, q)
    val sampleProbability = if (maxDocs == -1) 1.0 else math.min(maxDocs.toDouble / totalHits, 1.0)
    //logger.debug(f"q: $q%s, sampleProbability:$sampleProbability%,.4f <- maxDocs:$maxDocs%,d, hits:$totalHits%,d")
    tlc.get.setCollector(new SimpleCollector() {
      override def scoreMode = ScoreMode.COMPLETE_NO_SCORES
      var context: LeafReaderContext = _

      override def collect(doc: Int) {
        qm.documentsProcessed += 1
        if (maxDocs == -1 || sampleProbability == 1.0 || Math.random() < sampleProbability) {
          processedDocs+=1          
          val tv = this.context.reader.getTermVector(doc, ia.indexMetadata.contentField)
          if (tv != null) {
            docCollector(doc)
            if (tv.size()>10000) logger.debug(f"Long term vector for doc $doc%d: ${tv.size}%,d")
            val tvt = tv.iterator().asInstanceOf[TVTermsEnum]
            val min = if (ctvp.localScaling!=LocalTermVectorScaling.MIN) 0 else if (minScalingTerms.isEmpty) Int.MaxValue else minScalingTerms.foldLeft(0)((f,term) => if (tvt.seekExact(term)) f+tvt.totalTermFreq.toInt else f)
            var term = tvt.nextOrd()
            var anyMatches = false
            while (term != -1l) {
              processedTerms += 1
              term = termTransformer.map(s => {
                s.getBinding.setProperty("term", term)
                s.run().asInstanceOf[Long]
              }).getOrElse(term)
              if (ctvp.matches(it, term, tvt.totalTermFreq)) {
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
      override def doSetNextReader(context: LeafReaderContext) {
        this.context = context
        termTransformer.foreach(_.getBinding.invokeMethod("setContext", context))
        contextSetter(context)
      }
    })
    is.search(q, tlc.get)
    //logger.debug(f"$q%s, total docs: $totalHits%,d, processed docs: $processedDocs%,d, sample probability: $sampleProbability%,.2f. Contributing docs: $contributingDocs%,d, processed terms: $processedTerms%,d, accepted terms: $acceptedTerms%,d, total accepted term freq: $totalAcceptedTermFreq%,d")
    TermVectorQueryMetadata(totalHits, processedDocs, sampleProbability, contributingDocs, processedTerms, acceptedTerms, totalAcceptedTermFreq)
  }
  
  def getTermVectorForDocument(ir: IndexReader, it: TermsEnum, doc: Int, ctvpl: LocalTermVectorProcessingParameters, ctvpa: AggregateTermVectorProcessingParameters)(implicit ia: IndexAccess): LongDoubleMap = {
    val cv = HashLongIntMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap() 
    val tv = ir.getTermVector(doc, ia.indexMetadata.contentField)
    if (tv != null) {
      val tvt = tv.iterator.asInstanceOf[TVTermsEnum]
      var term = tvt.nextOrd()
      while (term != -1l) {
        if (ctvpl.matches(it, term, tvt.totalTermFreq))
          cv.addValue(term, tvt.totalTermFreq.toInt)
        term = tvt.nextOrd()
      }
    }
    scaleTermVector(cv, ctvpa, null, -1,-1)
  }

  private def getUnscaledAggregateContextVectorForQuery(is: IndexSearcher, it: TermsEnum, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[BytesRef], maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector], ia: IndexAccess, qm: QueryMetadata): (TermVectorQueryMetadata,LongIntMap) = {
     val cv = HashLongIntMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()     
     val md = runTermVectorQuery(is, it, q, ctvp, minScalingTerms, maxDocs, (_: LeafReaderContext) => Unit, (_: Int) => Unit, (term: Long, freq: Int) => cv.addValue(term, freq))
     (md,cv)
  }
  
  def limitTermVector(m: LongDoubleMap, ctvp: LimitParameters)(implicit ia: IndexAccess): LongDoubleMap = {
      val best = filterHighestScores(m, ctvp.limit)
      val m2 = HashLongDoubleMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()
      for ((key,value) <- best) m2.put(key, value)
      m2
  }
  
  private def scaleTermVector(cv: LongIntMap, ctvp: AggregateTermVectorProcessingParameters, te: TermsEnum, queryDocFreq: Long, totalDocFreq: Long)(implicit ia: IndexAccess): LongDoubleMap = {
    val m = HashLongDoubleMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()
    val ss = ctvp.sumScaling(te,queryDocFreq,totalDocFreq)
    cv.forEach(new LongIntConsumer {
       override def accept(k: Long, v: Int) {
         if (ctvp.matches(v)) m.put(k, ss(k, v))
       }
    })
    m
  }
  
  def getAggregateContextVectorForQuery(is: IndexSearcher, it: TermsEnum, q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[BytesRef], ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector], ia: IndexAccess, qm: QueryMetadata): (TermVectorQueryMetadata,LongDoubleMap) = {
    val (md, cv) = getUnscaledAggregateContextVectorForQuery(is, it, q, ctvpl, minScalingTerms, maxDocs)
    (md, scaleTermVector(cv, ctvpa,it,md.totalDocs,is.getIndexReader.numDocs))
  }
  
  private final class UnscaledVectorInfo {
    var docFreq = 0l
    var totalTermFreq = 0l
    val cv: LongIntMap = HashLongIntMaps.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableMap()
  }
  
  private def getGroupedUnscaledAggregateContextVectorsForQuery(level: LevelMetadata, is: IndexSearcher, it: TermsEnum, q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[BytesRef], grpp: GroupingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector], ia: IndexAccess, qm: QueryMetadata): (TermVectorQueryMetadata,collection.Map[Seq[JsValue], UnscaledVectorInfo]) = {
    val cvm = new mutable.HashMap[Seq[JsValue],UnscaledVectorInfo]
    var cv: UnscaledVectorInfo = null
    var fieldGetters: Seq[(Int) => JsValue] = null
    var anyMatches = false
    val tvm = runTermVectorQuery(is, it, q, ctvp, minScalingTerms, maxDocs, (nlrc: LeafReaderContext) => {
      fieldGetters = grpp.fields.map(level.fields(_).jsGetter(nlrc.reader).andThen(_.iterator.next))
    }, (doc: Int) => {
      if (anyMatches) cv.docFreq += 1
      cv = cvm.getOrElseUpdate(grpp.grouper.map(ap => {
        ap.invokeMethod("group", doc).asInstanceOf[java.util.List[Any]].asScala.map(v => if (v.isInstanceOf[JsValue]) v else JsString(v.asInstanceOf[String])).asInstanceOf[Seq[JsValue]]
      }).getOrElse(grpp.fieldTransformer.map(ap => {
        ap.getBinding.setProperty("fields", fieldGetters.map(_(doc)).asJava)
        ap.run().asInstanceOf[java.util.List[Any]].asScala.map(v => if (v.isInstanceOf[JsValue]) v else JsString(v.asInstanceOf[String])).asInstanceOf[Seq[JsValue]]
      }).getOrElse(if (grpp.fieldLengths.isEmpty) fieldGetters.map(_(doc)) else fieldGetters.zip(grpp.fieldLengths).map(p => {
        val value = p._1(doc).toString
        JsString(value.substring(0,Math.min(p._2,value.length)))
      }))), new UnscaledVectorInfo)
      anyMatches = false
    }, (term: Long, freq: Int) => {
        anyMatches = true
       cv.cv.addValue(term, freq)
       cv.totalTermFreq += freq
     })
    (tvm,cvm)
  }  

  final class VectorInfo(value: UnscaledVectorInfo, val cv: LongDoubleMap) {
    val docFreq = value.docFreq
    val totalTermFreq = value.totalTermFreq
  }

  def getGroupedAggregateContextVectorsForQuery(level: LevelMetadata, is: IndexSearcher, it: TermsEnum, q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[BytesRef], grpp: GroupingParameters, ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector], ia: IndexAccess, qm: QueryMetadata): (TermVectorQueryMetadata, collection.Map[Seq[JsValue], VectorInfo]) = {
    val (tvm, cvm) = getGroupedUnscaledAggregateContextVectorsForQuery(level, is, it, q, ctvpl, minScalingTerms, grpp, maxDocs)
    (tvm, cvm.map{ case (key,value) => (key, new VectorInfo(value,scaleTermVector(value.cv,ctvpa,it,tvm.totalDocs,is.getIndexReader.numDocs))) })
  }
  
  def getContextTermsForQuery(is: IndexSearcher, it: TermsEnum, q: Query, ctvp: LocalTermVectorProcessingParameters, maxDocs: Int)(implicit tlc: ThreadLocal[TimeLimitingCollector], ia: IndexAccess, qm: QueryMetadata): (TermVectorQueryMetadata,LongSet) = {
     val cv = HashLongSets.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableSet()
     val md = runTermVectorQuery(is, it, q, ctvp, Seq.empty, maxDocs, (_: LeafReaderContext) => Unit, (_: Int) => Unit, (term: Long, _) => cv.add(term))
     (md,cv)
  }  
  
  def filterHighestScores(cv: LongDoubleMap, limit: Int): Map[Long,Double] = {
    val maxHeap = mutable.PriorityQueue.empty[(Long,Double)]((x: (Long, Double), y: (Long, Double)) => y._2 compare x._2)
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
  
/*  private val f = classOf[ParallelBHTsne].getDeclaredField("gradientPool")
  private val f2 = classOf[ParallelBHTsne].getDeclaredField("gradientCalculationPool")
  f.setAccessible(true)
  f2.setAccessible(true)
    
  private val sr = {
    val il = classOf[MethodHandles.Lookup].getDeclaredField("IMPL_LOOKUP")
    il.setAccessible(true)
    val lkp: MethodHandles.Lookup = il.get(null).asInstanceOf[MethodHandles.Lookup]
    lkp.findSpecial(classOf[BHTSne], "run",MethodType.methodType(classOf[Array[Array[Double]]],classOf[TSneConfiguration]),classOf[ParallelBHTsne])
  } */
  
  def tsne(termVectors: Iterable[LongDoubleMap], rtp: TermVectorDimensionalityReductionParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = {
    val tvms = termVectors.toSeq
    val keys = HashLongSets.newUpdatableSet()
    for (tvm <- tvms) keys.addAll(tvm.keySet())
    val matrix = new Array[Array[Double]](tvms.size)
    for (i <- matrix.indices)
      matrix(i) = new Array[Double](keys.size)
    for (i <- matrix.indices) {
      var j = 0
      val itvm = tvms(i)
      val row = matrix(i)
      keys.forEach(new LongConsumer {
        override def accept(term: Long) {
          row(j) = itvm.getOrDefault(term, 0.0)
          j += 1
        }
      })
    }
/*    AllocUtil.setAllocationModeForContext(DataBuffer.AllocationMode.HEAP)
    DataTypeUtil.setDTypeForContext(DataBuffer.Type.DOUBLE)
    val tsne = new BarnesHutTsne.Builder()
      .setMaxIter(rtp.tsneMaxIter)
      .theta(rtp.tsneTheta)
      .normalize(true)
      .learningRate(500)
      .useAdaGrad(false)
      .numDimension(rtp.dimensions)
      .build()
    tsne.fit(Nd4j.create(matrix))
    val rd = tsne.getData
    val rm = new Array[Array[Double]](rd.rows)
    for (i <- 0 until rm.length) {
      rm(i) = new Array[Double](rd.columns)
      for (j <- 0 until rd.columns) rm(i)(j) = rd.getDouble(i, j)
    }
    println(rm.toSeq.map(_.toSeq))
    rm*/
    val tsne = new BHTSne()
    tsne.tsne(TSneUtils.buildConfig(matrix, rtp.dimensions, matrix.length, Math.min(matrix.length / 3 - 1, rtp.tsnePerplexity), rtp.tsneMaxIter, rtp.tsneUsePCA, rtp.tsneTheta, true))
    /*f.set(tsne, fjp)
    f2.set(tsne, ies)
    sr.invoke(tsne, TSneUtils.buildConfig(matrix, rtp.dimensions, matrix.length, Math.min(matrix.length / 3 - 1, rtp.tsnePerplexity), rtp.tsneMaxIter, rtp.tsneUsePCA, rtp.tsneTheta, true))*/
  }
  def mds(classical: Boolean, termVectors: Iterable[LongDoubleMap], rtp: TermVectorDimensionalityReductionParameters): Array[Array[Double]] = {
    val tvms = termVectors.toSeq
    val matrix = new Array[Array[Double]](tvms.size)
    for (i <- matrix.indices)
      matrix(i) = new Array[Double](matrix.length)
    for (i <- matrix.indices) {
      for (j <- i + 1 until matrix.length) {
        val dis = rtp.distance(tvms(i), tvms(j))
        matrix(i)(j) = dis
        matrix(j)(i) = dis
      }
    }
    (if (classical) MDSJ.classicalScaling(matrix, rtp.dimensions) else MDSJ.stressMinimization(matrix, rtp.dimensions)).transpose
  }
  
  def termOrdMapToStringMap(it: TermsEnum, m: LongDoubleMap)(implicit ia: IndexAccess): collection.Map[String,Double] = {
    val rm = new mutable.HashMap[String,Double]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm.put(termOrdToString(it, term),freq)
      }
    })
    rm
  }
  
  def termOrdMapToOrderedStringSeq(it: TermsEnum, m: LongDoubleMap)(implicit ia: IndexAccess): Seq[(String,Double)] = {
    val rm = new ArrayBuffer[(String,Double)]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm += ((termOrdToString(it, term),freq))
      }
    })
    rm.sortBy(-_._2)
  }
  
  def termOrdsToStrings(it: TermsEnum, m: LongSet)(implicit ia: IndexAccess): Traversable[String] = {
    new Traversable[String] {
      override def foreach[U](f: String => U): Unit = {
        m.forEach(new LongConsumer {
          override def accept(term: Long) {
            f(termOrdToString(it, term))
          }
        })
      }
    }
  }

  def termOrdMapToBytesRefMap(it: TermsEnum, m: LongDoubleMap)(implicit ia: IndexAccess): collection.Map[BytesRef,Double] = {
    val rm = new mutable.HashMap[BytesRef,Double]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm.put(termOrdToBytesRef(it, term),freq)
      }
    })
    rm
  }

  def termOrdMapToOrderedBytesRefSeq(it: TermsEnum, m: LongDoubleMap)(implicit ia: IndexAccess): Seq[(BytesRef,Double)] = {
    val rm = new ArrayBuffer[(BytesRef,Double)]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm += ((termOrdToBytesRef(it, term),freq))
      }
    })
    rm.sortBy(-_._2)
  }

  def termOrdsToBytesRefs(it: TermsEnum, m: LongSet)(implicit ia: IndexAccess): Traversable[BytesRef] = {
    new Traversable[BytesRef] {
      override def foreach[U](f: BytesRef => U): Unit = {
        m.forEach(new LongConsumer {
          override def accept(term: Long) {
            f(termOrdToBytesRef(it, term))
          }
        })
      }
    }
  }

}
