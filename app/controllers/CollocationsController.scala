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
class CollocationsController @Inject() (implicit ia: IndexAccess, materializer: Materializer, env: Environment) extends QueuingController(materializer, env) {
  
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
    val callId = s"collocations: $gp, $termVectorQueryParameters, $termVectorLocalProcessingParameters, $termVectorAggregateProcessingParameters, $resultTermVectorLimitQueryParameters, $resultTermVectorLocalProcessingParameters, $resultTermVectorAggregateProcessingParameters"
    val qm = Json.obj("method"->"collocations","callId"->callId,"termVector"->termVectors) ++ gp.toJson ++ termVectorQueryParameters.toJson ++ termVectorLocalProcessingParameters.toJson ++ termVectorAggregateProcessingParameters.toJson ++ resultTermVectorLimitQueryParameters.toJson ++ resultTermVectorLocalProcessingParameters.toJson ++ resultTermVectorAggregateProcessingParameters.toJson
    getOrCreateResult(callId, gp.force, () => {
      implicit val tlc = gp.tlc
      val (qlevel,termVectorQuery) = buildFinalQueryRunningSubQueries(termVectorQueryParameters.query.get)
      val is = searcher(qlevel, SumScaling.ABSOLUTE)
      val ir = is.getIndexReader
      val maxDocs = if (gp.maxDocs == -1 || termVectorAggregateProcessingParameters.limit == -1) -1 else if (resultTermVectorAggregateProcessingParameters.mdsDimensions>0 || resultTermVectorLocalProcessingParameters.defined || resultTermVectorAggregateProcessingParameters.defined || termVectors) gp.maxDocs / (termVectorAggregateProcessingParameters.limit + 1) else gp.maxDocs / 2
      val (md, allCollocations) = getAggregateContextVectorForQuery(is, termVectorQuery,termVectorLocalProcessingParameters,extractContentTermsFromQuery(termVectorQuery),termVectorAggregateProcessingParameters,maxDocs)
      val collocations = filterHighestScores(allCollocations, termVectorAggregateProcessingParameters.limit)
      val json = Json.obj("queryMetadata"->qm, "results"->Json.obj("metadata"->md.toJson,"terms"->(if (resultTermVectorAggregateProcessingParameters.mdsDimensions>0) {
        val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(_)._2)
        val ctermVectors = collocations.keys.toSeq.par.map{term => 
          val termS = termOrdToTerm(ir,term)
          val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term("content",termS)), Occur.MUST)
          for (q <- resultLimitQuery) bqb.add(q, Occur.MUST)
          getAggregateContextVectorForQuery(is, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs)
        }.seq
        val mdsMatrix = mds(ctermVectors.map(_._2),resultTermVectorAggregateProcessingParameters)
        collocations.zipWithIndex.toSeq.sortBy(-_._1._2).map{ case ((term,weight),i) => Json.obj("term"->termOrdToTerm(ir, term), "termVector"->Json.obj("metadata"->ctermVectors(i)._1.toJson,"terms"->mdsMatrix(i)), "weight"->weight)}
      } else if (resultTermVectorLocalProcessingParameters.defined || resultTermVectorAggregateProcessingParameters.defined || termVectors) {
        val resultLimitQuery = resultTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(_)._2)
        collocations.toSeq.sortBy(-_._2).par.map{ case (term, weight) => {
          val termS = termOrdToTerm(ir,term)
          val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term("content",termS)), Occur.MUST)
          for (q <- resultLimitQuery) bqb.add(q, Occur.MUST)
          val (md, ctermVector) = getAggregateContextVectorForQuery(is, bqb.build(), resultTermVectorLocalProcessingParameters, Seq(termS), resultTermVectorAggregateProcessingParameters, maxDocs) 
          Json.obj("term" -> termS, "termVector"->Json.obj("metadata"->md.toJson, "terms"->termOrdMapToOrderedTermSeq(ir, ctermVector).map(p=>Json.obj("term" -> p._1, "weight" -> p._2)),"weight"->weight))
        }}.seq
      } else collocations.toSeq.sortBy(-_._2).map(p => Json.obj("term"->termOrdToTerm(ir,p._1), "weight" -> p._2)))))
      if (gp.pretty)
        Ok(Json.prettyPrint(json))
      else 
        Ok(json)
    })
  }
}