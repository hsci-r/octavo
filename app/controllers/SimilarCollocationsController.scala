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
import org.apache.lucene.search.Query
import scala.collection.mutable.ArrayBuffer
import com.koloboke.function.LongDoubleConsumer
import scala.concurrent.Await
import scala.concurrent.Future
import com.koloboke.collect.set.hash.HashLongSets
import scala.concurrent.duration.Duration
import scala.collection.mutable.PriorityQueue
import com.koloboke.collect.set.LongSet
import java.util.function.LongConsumer
import services.Distance
import services.IndexAccessProvider
import play.api.Configuration

@Singleton
class SimilarCollocationsController @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  import TermVectors._
  
  // get terms with similar collocations for a term - to find out what other words are talked about in a similar manner, for topic definition
  def similarCollocations(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = new GeneralParameters
    implicit val iec = gp.executionContext
    val termVectorQueryParameters = QueryParameters()
    val termVectorLocalProcessingParameters = LocalTermVectorProcessingParameters()
    val termVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters()
/*    val comparisonTermVectorQueryParameters = QueryParameters("c_")
    val comparisonTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("c_")
    val comparisonTermVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters("c_") */
    val intermediaryTermVectorLimitQueryParameters = QueryParameters("i_")
    val intermediaryTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("i_")
    val finalTermVectorLimitQueryParameters = QueryParameters("f_")
    val finalTermVectorLocalProcessingParameters = LocalTermVectorProcessingParameters("f_")
    val finalTermVectorAggregateProcessingParameters = AggregateTermVectorProcessingParameters("f_")
    val qm = Json.obj("method"->"similarCollocations") ++ gp.toJson ++ termVectorQueryParameters.toJson ++ termVectorLocalProcessingParameters.toJson ++ termVectorAggregateProcessingParameters.toJson ++ intermediaryTermVectorLimitQueryParameters.toJson ++ intermediaryTermVectorLocalProcessingParameters.toJson ++ finalTermVectorLimitQueryParameters.toJson ++ finalTermVectorLocalProcessingParameters.toJson ++ finalTermVectorAggregateProcessingParameters.toJson  
    getOrCreateResult(ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      val (qlevel,termVectorQuery) = buildFinalQueryRunningSubQueries(termVectorQueryParameters.requiredQuery)
      val is = searcher(qlevel, SumScaling.ABSOLUTE)
      val ir = is.getIndexReader
      val maxDocs2 = if (gp.maxDocs == -1) -1 else gp.maxDocs / 3
      val (_,collocations) = getAggregateContextVectorForQuery(is, termVectorQuery, termVectorLocalProcessingParameters,extractContentTermsFromQuery(termVectorQuery),termVectorAggregateProcessingParameters, maxDocs2)
      println("collocations: "+collocations.size)
      val futures = new ArrayBuffer[Future[LongSet]]
      val maxDocs3 = if (gp.maxDocs == -1) -1 else maxDocs2/collocations.size
      val intermediaryLimitQuery = intermediaryTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(_)._2)
      collocations.forEach(new LongDoubleConsumer {
        override def accept(term: Long, freq: Double) {
          val termS = termOrdToTerm(ir, term)
          val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField,termS)), Occur.MUST)
          for (q <- intermediaryLimitQuery) bqb.add(q, Occur.MUST)
          futures += Future {
            val (_,tv) = getContextTermsForQuery(is, bqb.build, intermediaryTermVectorLocalProcessingParameters, maxDocs3)
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
      println("collocations of collocations: "+collocationCollocations.size)
      val maxDocs4 = if (gp.maxDocs == -1) -1 else maxDocs2/collocationCollocations.size
      val finalLimitQuery = finalTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(_)._2)
      val thirdOrderCollocations = for (term2 <- termOrdsToTerms(ir, collocationCollocations).par) yield {
        val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField,term2)), Occur.MUST)
        for (q <- finalLimitQuery) bqb.add(q, Occur.MUST)
        val (_,tv) = getAggregateContextVectorForQuery(is, bqb.build,finalTermVectorLocalProcessingParameters,Seq(term2), finalTermVectorAggregateProcessingParameters, maxDocs4)
        (term2,tv)
      }
      println("third order collocations:"+thirdOrderCollocations.size)
      val ordering = new Ordering[(String,Double)] {
        override def compare(x: (String,Double), y: (String,Double)) = y._2 compare x._2
      }
      val cmaxHeap = PriorityQueue.empty[(String,Double)](ordering)
      val dmaxHeap = PriorityQueue.empty[(String,Double)](ordering)
      var total = 0
      val termsToScores = thirdOrderCollocations.filter(!_._2.isEmpty).map(p => (p._1,Distance.cosineSimilarity(collocations,p._2),Distance.diceSimilarity(collocations,p._2)))
      for ((term,cscore,dscore) <- termsToScores.seq) {
        total+=1
        if (finalTermVectorAggregateProcessingParameters.limit == -1 || total<=finalTermVectorAggregateProcessingParameters.limit) { 
          if (cscore!=0.0) cmaxHeap += ((term,cscore))
          if (dscore!=0.0) dmaxHeap += ((term,dscore))
        } else {
          if (cmaxHeap.head._2<cscore && cscore!=0.0) {
            cmaxHeap.dequeue()
            cmaxHeap += ((term,cscore))
          }
          if (dmaxHeap.head._2<dscore && dscore!=0.0) {
            dmaxHeap.dequeue()
            dmaxHeap += ((term,dscore))
          }
        }
      }
      Json.toJson(Map("cosine"->cmaxHeap.toMap,"dice"->dmaxHeap.toMap)) 
    })
  }
  
}