package controllers

import java.util.function.LongConsumer

import com.koloboke.collect.set.LongSet
import com.koloboke.collect.set.hash.HashLongSets
import com.koloboke.function.LongDoubleConsumer
import javax.inject.{Inject, Singleton}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, TermQuery}
import parameters._
import play.api.Logging
import play.api.libs.json.{JsObject, Json}
import services.{Distance, IndexAccessProvider, TermVectors}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@Singleton
class SimilarCollocationsController @Inject() (implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) with Logging {
  
  import TermVectors._
  
  // get terms with similar collocations for a term - to find out what other words are talked about in a similar manner, for topic definition
  def similarCollocations(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    implicit val qm = new QueryMetadata() {
      var phase = ""
      var collocations = -1
      var secondOrderCollocations = -1
      var thirdOrderCollocations = -1
      override def status: JsObject = super.status ++ Json.obj("phase"->phase, "collocations"->collocations,"secondOrderCollocations"->secondOrderCollocations,"thirdOrderCollocations"->thirdOrderCollocations)
    }
    import ia._
    val gp = new GeneralParameters
    implicit val iec = gp.executionContext
    implicit val its = gp.taskSupport
    val limitParameters = new LimitParameters()
    val termVectorQueryParameters = new QueryParameters()
    val termVectorLocalProcessingParameters = new LocalTermVectorProcessingParameters()
    val termVectorAggregateProcessingParameters = new AggregateTermVectorProcessingParameters()
    val termVectorSamplingParameters = new SamplingParameters()
/*    val comparisonTermVectorQueryParameters = QueryParameters("c_")
    val comparisonTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("c_")
    val comparisonTermVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters("c_") */
    val intermediaryTermVectorLimitQueryParameters = new QueryParameters("i_")
    val intermediaryTermVectorLocalProcessingParameters = new LocalTermVectorProcessingParameters("i_")
    val intermediaryTermVectorSamplingParameters = new SamplingParameters("i_")
    val finalTermVectorLimitQueryParameters = new QueryParameters("f_")
    val finalTermVectorLocalProcessingParameters = new LocalTermVectorProcessingParameters("f_")
    val finalTermVectorAggregateProcessingParameters = new AggregateTermVectorProcessingParameters("f_")
    val finalTermVectorDistanceCalculationParameters = new TermVectorDistanceCalculationParameters("f_")
    val finalTermVectorSamplingParameters = new SamplingParameters("f_")
    implicit val tlc = gp.tlc
    getOrCreateResult("similarCollocations",ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      // FIXME
    }, () => {
      qm.phase="termVectors"
      val (qlevel,termVectorQuery) = buildFinalQueryRunningSubQueries(exactCounts = false, termVectorQueryParameters.requiredQuery)
      val is = searcher(qlevel, SumScaling.ABSOLUTE)
      val ir = is.getIndexReader
      val (_,collocations) = getAggregateContextVectorForQuery(is, termVectorQuery, termVectorLocalProcessingParameters,extractContentTermsFromQuery(termVectorQuery),termVectorAggregateProcessingParameters, termVectorSamplingParameters.maxDocs)
      qm.collocations = collocations.size
      logger.debug("Collocations: "+collocations.size)
      qm.phase = "secondOrderCollocations"
      val futures = new ArrayBuffer[Future[LongSet]]
      val maxDocs3 = if (intermediaryTermVectorSamplingParameters.maxDocs == -1) -1 else intermediaryTermVectorSamplingParameters.maxDocs/collocations.size
      if (maxDocs3 == 0) Right(BadRequest("i_maxDocs of "+intermediaryTermVectorSamplingParameters.maxDocs+" results in 0 samples for collocation set of size "+collocations.size)) else {
        val intermediaryLimitQuery = intermediaryTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
        collocations.forEach(new LongDoubleConsumer {
          override def accept(term: Long, freq: Double) {
            val termS = termOrdToTerm(ir, term)
            val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField, termS)), Occur.FILTER)
            for (q <- intermediaryLimitQuery) bqb.add(q, Occur.FILTER)
            futures += Future {
              val (_, tv) = getContextTermsForQuery(is, bqb.build, intermediaryTermVectorLocalProcessingParameters, maxDocs3)
              tv
            }
          }
        })
        val collocationCollocations = HashLongSets.getDefaultFactory.withKeysDomain(0, Long.MaxValue).newUpdatableSet() // tvs for all terms in the tv of the query
        for (set <- Await.result(Future.sequence(futures), Duration.Inf))
          set.forEach(new LongConsumer() {
            override def accept(term: Long) {
              collocationCollocations.add(term)
            }
          })
        logger.info("Collocations of collocations: " + collocationCollocations.size)
        qm.secondOrderCollocations = collocationCollocations.size
        qm.phase = "thirdOrderCollocations"
        qm.thirdOrderCollocations = 0
        val maxDocs4 = if (finalTermVectorSamplingParameters.maxDocs == -1) -1 else finalTermVectorSamplingParameters.maxDocs / collocationCollocations.size
        if (maxDocs4 == 0) Right(BadRequest("f_maxDocs of "+intermediaryTermVectorSamplingParameters.maxDocs+" results in 0 samples for collocation set of size "+collocationCollocations.size)) else {
          val finalLimitQuery = finalTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
          val thirdOrderCollocations = for (term <- toParallel(termOrdsToTerms(ir, collocationCollocations))) yield {
            val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField, term)), Occur.FILTER)
            for (q <- finalLimitQuery) bqb.add(q, Occur.FILTER)
            val (_, tv) = getAggregateContextVectorForQuery(is, bqb.build, finalTermVectorLocalProcessingParameters, Seq(term), finalTermVectorAggregateProcessingParameters, maxDocs4)
            qm.thirdOrderCollocations += 1
            if (tv.isEmpty) (term, 1.0, 1.0, 1.0, 1.0, 1.0) else (
              term,
              1.0-Distance.cosineSimilarity(collocations, tv, finalTermVectorDistanceCalculationParameters.filtering),
              1.0-Distance.diceSimilarity(collocations, tv),
              1.0-Distance.jaccardSimilarity(collocations, tv),
              Distance.euclideanDistance(collocations, tv, finalTermVectorDistanceCalculationParameters.filtering),
              Distance.manhattanDistance(collocations, tv, finalTermVectorDistanceCalculationParameters.filtering)
            )
          }
          val ordering = new Ordering[(String, Double)] {
            override def compare(x: (String, Double), y: (String, Double)) = x._2 compare y._2
          }
          val cmaxHeap = collection.mutable.PriorityQueue.empty[(String, Double)](ordering)
          val dmaxHeap = collection.mutable.PriorityQueue.empty[(String, Double)](ordering)
          val jmaxHeap = collection.mutable.PriorityQueue.empty[(String, Double)](ordering)
          val emaxHeap = collection.mutable.PriorityQueue.empty[(String, Double)](ordering)
          val mmaxHeap = collection.mutable.PriorityQueue.empty[(String, Double)](ordering)
          for ((term,cscore,dscore,jscore,escore,mscore) <- thirdOrderCollocations.seq) {
            if (limitParameters.limit == -1 || cmaxHeap.size <= limitParameters.limit) {
              cmaxHeap += ((term, cscore))
              dmaxHeap += ((term, dscore))
              jmaxHeap += ((term, jscore))
              emaxHeap += ((term, escore))
              mmaxHeap += ((term, mscore))
            } else {
              if (cmaxHeap.head._2 > cscore) {
                cmaxHeap.dequeue()
                cmaxHeap += ((term, cscore))
              }
              if (dmaxHeap.head._2 > dscore) {
                dmaxHeap.dequeue()
                dmaxHeap += ((term, dscore))
              }
              if (jmaxHeap.head._2 > jscore) {
                jmaxHeap.dequeue()
                jmaxHeap += ((term, jscore))
              }
              if (emaxHeap.head._2 > escore) {
                emaxHeap.dequeue()
                emaxHeap += ((term, escore))
              }
              if (mmaxHeap.head._2 > mscore) {
                mmaxHeap.dequeue()
                mmaxHeap += ((term, mscore))
              }
            }
          }
          Left(Json.obj(
            "cosine" -> cmaxHeap.toSeq.sortBy(_._2).map(p => Json.obj("term"->p._1,"distance"->p._2)),
            "dice" -> dmaxHeap.toSeq.sortBy(_._2).map(p => Json.obj("term"->p._1,"distance"->p._2)),
            "jaccard"-> jmaxHeap.toSeq.sortBy(_._2).map(p => Json.obj("term"->p._1,"distance"->p._2)),
            "euclidean" -> emaxHeap.toSeq.sortBy(_._2).map(p => Json.obj("term"->p._1,"distance"->p._2)),
            "manhattan" -> mmaxHeap.toSeq.sortBy(_._2).map(p => Json.obj("term"->p._1,"distance"->p._2))
          ))
        }
      }
    })
  }
  
}