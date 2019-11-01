package controllers

import com.koloboke.function.LongDoubleConsumer
import javax.inject.{Inject, Singleton}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, TermInSetQuery, TermQuery}
import org.apache.lucene.util.BytesRef
import parameters._
import play.api.Logging
import play.api.libs.json.{JsObject, Json}
import services.{Distance, IndexAccess, IndexAccessProvider, TermVectors}

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
    import IndexAccess._
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
      val is = searcher(qlevel, QueryScoring.NONE)
      val it = termsEnums(qlevel).get
      val (cmd,collocations) = getAggregateContextVectorForQuery(is, it, termVectorQuery, termVectorLocalProcessingParameters,extractContentTermBytesRefsFromQuery(termVectorQuery),termVectorAggregateProcessingParameters, termVectorSamplingParameters.maxDocs)
      qm.collocations = collocations.size
      logger.debug("Collocations: "+collocations.size)
      qm.phase = "secondOrderCollocations"
      val maxDocs3 = if (intermediaryTermVectorSamplingParameters.maxDocs == -1) -1 else intermediaryTermVectorSamplingParameters.maxDocs
      if (maxDocs3 == 0) Right(BadRequest("i_maxDocs of "+intermediaryTermVectorSamplingParameters.maxDocs+" results in 0 samples for collocation set of size "+collocations.size)) else {
        val intermediaryLimitQuery = intermediaryTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
        val terms = new Array[BytesRef](collocations.size)
        var i = 0
        collocations.forEach(new LongDoubleConsumer {
          override def accept(term: Long, freq: Double): Unit = {
            terms(i) = termOrdToBytesRef(it, term)
            i += 1
          }
        })
        val bqb = new BooleanQuery.Builder().add(new TermInSetQuery(indexMetadata.contentField,terms:_*), Occur.FILTER)
        for (q <- intermediaryLimitQuery) bqb.add(q, Occur.FILTER)
        val (scmd, collocationCollocations) = getContextTermsForQuery(is, it, bqb.build, intermediaryTermVectorLocalProcessingParameters, maxDocs3)
        logger.debug("Collocations of collocations: " + collocationCollocations.size)
        qm.secondOrderCollocations = collocationCollocations.size
        qm.phase = "thirdOrderCollocations"
        qm.thirdOrderCollocations = 0
        val maxDocs4 = if (finalTermVectorSamplingParameters.maxDocs == -1) -1 else finalTermVectorSamplingParameters.maxDocs / collocationCollocations.size
        var tcmd: TermVectorQueryMetadata = null
        if (maxDocs4 == 0) Right(BadRequest("f_maxDocs of "+finalTermVectorSamplingParameters.maxDocs+" results in 0 samples for collocation set of size "+collocationCollocations.size)) else {
          val finalLimitQuery = finalTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
          val thirdOrderCollocations = for (term <- toParallel(termOrdsToBytesRefs(it, collocationCollocations))) yield {
            val it2 = termsEnums(qlevel).get
            val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField, term)), Occur.FILTER)
            for (q <- finalLimitQuery) bqb.add(q, Occur.FILTER)
            val (ntcmd, tv) = getAggregateContextVectorForQuery(is, it2, bqb.build, finalTermVectorLocalProcessingParameters, Seq(term), finalTermVectorAggregateProcessingParameters, maxDocs4)
            if (tcmd!=null) tcmd = tcmd.combine(ntcmd) else tcmd = ntcmd
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
          val ordering = new Ordering[(BytesRef, Double)] {
            override def compare(x: (BytesRef, Double), y: (BytesRef, Double)) = x._2 compare y._2
          }
          val cmaxHeap = collection.mutable.PriorityQueue.empty[(BytesRef, Double)](ordering)
          val dmaxHeap = collection.mutable.PriorityQueue.empty[(BytesRef, Double)](ordering)
          val jmaxHeap = collection.mutable.PriorityQueue.empty[(BytesRef, Double)](ordering)
          val emaxHeap = collection.mutable.PriorityQueue.empty[(BytesRef, Double)](ordering)
          val mmaxHeap = collection.mutable.PriorityQueue.empty[(BytesRef, Double)](ordering)
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
            "metadata" -> Json.obj(
              "collocations" -> (cmd.toJson ++ Json.obj("size"->qm.collocations)),
              "secondOrderCollocations" -> (scmd.toJson ++ Json.obj("size"->qm.secondOrderCollocations)),
              "thirdOrderCollocations" -> tcmd.toJson
             ),
           "cosine" -> cmaxHeap.toSeq.sortBy(_._2)(Ordering.Double.TotalOrdering).map(p => Json.obj("term"->p._1.utf8ToString,"distance"->p._2)),
            "dice" -> dmaxHeap.toSeq.sortBy(_._2)(Ordering.Double.TotalOrdering).map(p => Json.obj("term"->p._1.utf8ToString,"distance"->p._2)),
            "jaccard"-> jmaxHeap.toSeq.sortBy(_._2)(Ordering.Double.TotalOrdering).map(p => Json.obj("term"->p._1.utf8ToString,"distance"->p._2)),
            "euclidean" -> emaxHeap.toSeq.sortBy(_._2)(Ordering.Double.TotalOrdering).map(p => Json.obj("term"->p._1.utf8ToString,"distance"->p._2)),
            "manhattan" -> mmaxHeap.toSeq.sortBy(_._2)(Ordering.Double.TotalOrdering).map(p => Json.obj("term"->p._1.utf8ToString,"distance"->p._2))
          ))
        }
      }
    })
  }
  
}