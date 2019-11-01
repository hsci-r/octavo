package controllers

import com.koloboke.collect.map.LongDoubleMap
import com.koloboke.function.LongDoubleConsumer
import javax.inject.{Inject, Singleton}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, TermQuery, TotalHitCountCollector}
import parameters._
import play.api.libs.json.{JsObject, Json}
import services.{IndexAccess, IndexAccessProvider, TermVectors}

import scala.collection.mutable.ArrayBuffer

@Singleton
class TermVectorsController @Inject()(implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  import TermVectors._
  import IndexAccess._

  // get collocations for a term query (+ a possible limit query), for defining a topic
  def termVectors(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val termVectors = p.get("termVectors").exists(v => v.head=="" || v.head.toBoolean)
    val distances = p.get("distances").exists(v => v.head=="" || v.head.toBoolean)
    val fullJson = Json.obj("distances"->distances,"termVectors"->termVectors)
    implicit val qm = new QueryMetadata(JsObject(fullJson.fields.filter(pa => p.get(pa._1).isDefined)), fullJson)
    val gp = new GeneralParameters()
    val grpp = new GroupingParameters()
    val termVectorQueryParameters = new QueryParameters()
    val termVectorLocalProcessingParameters = new LocalTermVectorProcessingParameters()
    val termVectorAggregateProcessingParameters = new AggregateTermVectorProcessingParameters()
    val termVectorLimitParameters = new LimitParameters()
    val termVectorSamplingParameters = new SamplingParameters()
/*    val comparisonTermVectorQueryParameters = QueryParameters("c_")
    val comparisonTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("c_")
    val comparisonTermVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters("c_") */
    val resultTermVectorLimitQueryParameters = new QueryParameters("r_")
    val resultTermVectorLocalProcessingParameters = new LocalTermVectorProcessingParameters("r_")
    val resultTermVectorAggregateProcessingParameters = new AggregateTermVectorProcessingParameters("r_")
    val resultTermVectorDimensionalityReductionParameters = new TermVectorDimensionalityReductionParameters("r_")
    //val resultTermVectorGroupingParameters = new GroupingParameters("r_")
    val resultTermVectorLimitParameters = new LimitParameters("r_")
    val termVectorDistanceCalculationParameters = new TermVectorDistanceCalculationParameters()
    implicit val iec = gp.executionContext
    implicit val tlc = gp.tlc
    getOrCreateResult("termVectors",ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      val qhits = {
        val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = false, termVectorQueryParameters.requiredQuery)
        val hc = new TotalHitCountCollector()
        gp.etlc.setCollector(hc)
        searcher(qlevel, QueryScoring.NONE).search(query, gp.etlc)
        hc.getTotalHits
      }
      qm.estimatedDocumentsToProcess = qhits
      if (resultTermVectorDimensionalityReductionParameters.dimensions>0 || termVectors || distances) qm.estimatedDocumentsToProcess += qhits * termVectorLimitParameters.limit
      qm.estimatedNumberOfResults = Math.min(qhits, termVectorLimitParameters.limit)
    }, () => {
      implicit val its = gp.taskSupport
      implicit val ifjp = gp.forkJoinPool
      val (qlevel, termVectorQuery) = buildFinalQueryRunningSubQueries(exactCounts = false, termVectorQueryParameters.requiredQuery)
      val is = searcher(qlevel, QueryScoring.NONE)
      // val ir = is.getIndexReader
      val maxDocs = if (termVectorSamplingParameters.maxDocs == -1 || termVectorLimitParameters.limit == -1) -1 else if (termVectors) termVectorSamplingParameters.maxDocs / (termVectorLimitParameters.limit + 1) else termVectorSamplingParameters.maxDocs / 2
      val processResults = (collocationsMap: LongDoubleMap) => {
        val collocations = new ArrayBuffer[(Long, Double)]
        limitTermVector(collocationsMap, termVectorLimitParameters).forEach(new LongDoubleConsumer {
          override def accept(k: Long, v: Double): Unit = {
            collocations += ((k, v))
          }
        })
        val it = termsEnums(qlevel).get
        val scollocations = collocations.map(p => (termOrdToBytesRef(it, p._1), p._2))
        if (resultTermVectorDimensionalityReductionParameters.dimensions > 0) {
          val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
          val ctermVectors = toParallel(scollocations).map { case (termS,_) =>
            val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField, termS)), Occur.FILTER)
            for (q <- resultLimitQuery) bqb.add(q, Occur.FILTER)
            getAggregateContextVectorForQuery(is, termsEnums(qlevel).get, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs)
          }.seq.toSeq
          val mdsMatrix = resultTermVectorDimensionalityReductionParameters.dimensionalityReduction(ctermVectors.map(p => limitTermVector(p._2, resultTermVectorLimitParameters)), resultTermVectorDimensionalityReductionParameters)
          scollocations.zipWithIndex.sortBy(-_._1._2)(Ordering.Double.TotalOrdering).map { case ((term, weight), i) =>
            val r = Json.obj("term" -> term.utf8ToString, "termVector" -> Json.obj("metadata" -> ctermVectors(i)._1.toJson, "terms" -> mdsMatrix(i)), "weight" -> weight)
            if (distances)
              r ++ Json.obj("distance" -> termVectorDistanceCalculationParameters.distance(collocationsMap, ctermVectors(i)._2))
            else r
          }
        } else if (termVectors) {
          val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
          toParallel(scollocations.sortBy(-_._2)(Ordering.Double.TotalOrdering)).map { case (termS, weight) =>
            val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField, termS)), Occur.FILTER)
            for (q <- resultLimitQuery) bqb.add(q, Occur.FILTER)
            val (md, ctermVector) = getAggregateContextVectorForQuery(is, termsEnums(qlevel).get, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs)
            val r = Json.obj("term" -> termS.utf8ToString, "termVector" -> Json.obj("metadata" -> md.toJson, "terms" -> termOrdMapToOrderedStringIterable(termsEnums(qlevel).get, limitTermVector(ctermVector, resultTermVectorLimitParameters)).map(p => Json.obj("term" -> p._1, "weight" -> p._2)), "weight" -> weight))
            if (distances)
              r ++ Json.obj("distance" -> termVectorDistanceCalculationParameters.distance(collocationsMap, ctermVector))
            else r
          }.seq
        } else if (distances) {
          val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false, _)._2)
          toParallel(scollocations.sortBy(-_._2)(Ordering.Double.TotalOrdering)).map { case (termS, weight) =>
            val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField, termS)), Occur.FILTER)
            for (q <- resultLimitQuery) bqb.add(q, Occur.FILTER)
            val (_, ctermVector) = getAggregateContextVectorForQuery(is, termsEnums(qlevel).get, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs)
            Json.obj("term" -> termS.utf8ToString, "weight" -> weight, "distance" -> termVectorDistanceCalculationParameters.distance(collocationsMap, ctermVector))
          }.seq
        } else scollocations.sortBy(-_._2)(Ordering.Double.TotalOrdering).map(p => Json.obj("term" -> p._1.utf8ToString, "weight" -> p._2))
      }
      Left(if (grpp.isDefined) {
        val (md, groupedCollocations) = getGroupedAggregateContextVectorsForQuery(ia.indexMetadata.levelMap(qlevel), is, termsEnums(qlevel).get, termVectorQuery, termVectorLocalProcessingParameters, extractContentTermBytesRefsFromQuery(termVectorQuery), grpp, termVectorAggregateProcessingParameters, maxDocs)
        Json.obj("metadata" -> md.toJson, "groups"-> groupedCollocations.toSeq.map(p => {
          Json.obj("fields"->p._1,"stats"->Json.obj("totalTermFreq"->p._2.totalTermFreq,"docFreq"->p._2.docFreq,"terms"->processResults(p._2.cv)))
        }))
      } else {
        val (md, collocationsMap) = getAggregateContextVectorForQuery(is, termsEnums(qlevel).get, termVectorQuery, termVectorLocalProcessingParameters, extractContentTermBytesRefsFromQuery(termVectorQuery), termVectorAggregateProcessingParameters, maxDocs)
        Json.obj("metadata" -> md.toJson, "terms" -> processResults(collocationsMap))
      })
    })
  }
}