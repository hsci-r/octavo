package controllers

import javax.inject.Singleton
import javax.inject.Inject
import services.IndexAccess
import akka.stream.Materializer
import play.api.Environment
import parameters.GeneralParameters
import play.api.mvc.Action
import parameters.QueryParameters
import parameters.AggregateTermVectorProcessingParameters
import parameters.LocalTermVectorProcessingParameters
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.TermQuery
import org.apache.lucene.index.Term
import play.api.libs.json.Json
import parameters.SumScaling
import org.apache.lucene.search.BooleanClause.Occur
import services.TermVectors

@Singleton
class CollocationsController @Inject() (ia: IndexAccess, materializer: Materializer, env: Environment) extends QueuingController(materializer, env) {
  
  import IndexAccess._
  import ia._
  import TermVectors._
  
  // get collocations for a term query (+ a possible limit query), for defining a topic
  def collocations() = Action { implicit request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = GeneralParameters()
    val termVectorQueryParameters = QueryParameters()
    val termVectorLocalProcessingParameters = LocalTermVectorProcessingParameters()
    val termVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters()
/*    val comparisonTermVectorQueryParameters = QueryParameters("c_")
    val comparisonTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("c_")
    val comparisonTermVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters("c_") */
    val resultTermVectorLimitQueryParameters = QueryParameters("r_")
    val resultTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("r_")
    val resultTermVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters("r_")
    val termVectors = p.get("termVector").exists(v => v(0)=="" || v(0).toBoolean)
    implicit val iec = gp.executionContext
    getOrCreateResult(s"collocations: $gp, $termVectorQueryParameters, $termVectorLocalProcessingParameters, $termVectorAggregateProcessingParameters, $resultTermVectorLimitQueryParameters, $resultTermVectorLocalProcessingParameters, $resultTermVectorAggregateProcessingParameters", gp.force, () => {
      implicit val tlc = gp.tlc
      val (qlevel,termVectorQuery) = buildFinalQueryRunningSubQueries(termVectorQueryParameters.query.get)
      val is = searcher(qlevel, SumScaling.ABSOLUTE)
      val ir = is.getIndexReader
      val maxDocs = if (gp.maxDocs == -1 || gp.limit == -1) -1 else if (resultTermVectorAggregateProcessingParameters.mdsDimensions>0 || resultTermVectorLocalProcessingParameters.defined || resultTermVectorAggregateProcessingParameters.defined || termVectors) gp.maxDocs / (gp.limit + 1) else gp.maxDocs / 2
      val (docFreq, totalTermFreq, allCollocations) = getAggregateContextVectorForQuery(is, termVectorQuery,termVectorLocalProcessingParameters,extractContentTermsFromQuery(termVectorQuery),termVectorAggregateProcessingParameters,maxDocs)
      val collocations = filterHighestScores(allCollocations, gp.limit)
      val json = Json.toJson(Map("processedDocFreq"->Json.toJson(docFreq),"processedTotalTermFreq"->Json.toJson(totalTermFreq),"terms"->(if (resultTermVectorAggregateProcessingParameters.mdsDimensions>0) {
        val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(_)._2)
        val mdsMatrix = mds(collocations.keys.toSeq.par.map{term => 
          val termS = termOrdToTerm(ir,term)
          val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term("content",termS)), Occur.MUST)
          for (q <- resultLimitQuery) bqb.add(q, Occur.MUST)
          getAggregateContextVectorForQuery(is, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs)._3
        }.seq,resultTermVectorAggregateProcessingParameters)
        Json.toJson(collocations.zipWithIndex.toSeq.sortBy(-_._1._2).map{ case ((term,weight),i) => Json.toJson(Map("term"->Json.toJson(termOrdToTerm(ir, term)), "termVector"->Json.toJson(mdsMatrix(i)), "weight"->Json.toJson(weight)))})
      } else if (resultTermVectorLocalProcessingParameters.defined || resultTermVectorAggregateProcessingParameters.defined || termVectors) {
        val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(_)._2)
        Json.toJson(collocations.toSeq.sortBy(-_._2).par.map{ case (term, weight) => {
          val termS = termOrdToTerm(ir,term)
          val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term("content",termS)), Occur.MUST)
          for (q <- resultLimitQuery) bqb.add(q, Occur.MUST)
          Map("term" -> Json.toJson(termS), "termVector"->Json.toJson(termOrdMapToTermMap(ir, getAggregateContextVectorForQuery(is, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs)._3)),"weight"->Json.toJson(weight))
        }}.seq)
      } else Json.toJson(collocations.toSeq.sortBy(-_._2).map(p => Map("term"->Json.toJson(termOrdToTerm(ir,p._1)), "weight" -> Json.toJson(p._2)))))))
      if (gp.pretty)
        Ok(Json.prettyPrint(json))
      else 
        Ok(json)
    })
  }
}