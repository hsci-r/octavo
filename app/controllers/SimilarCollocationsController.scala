package controllers

import java.util.function.LongConsumer
import javax.inject.{Inject, Singleton}

import com.koloboke.collect.set.LongSet
import com.koloboke.collect.set.hash.HashLongSets
import com.koloboke.function.LongDoubleConsumer
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, TermQuery}
import parameters._
import play.api.{Configuration, Environment}
import play.api.libs.json.Json
import services.{Distance, IndexAccessProvider, TermVectors}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

@Singleton
class SimilarCollocationsController @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  import TermVectors._
  
  // get terms with similar collocations for a term - to find out what other words are talked about in a similar manner, for topic definition
  def similarCollocations(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    implicit val qm = new QueryMetadata()
    import ia._
    val gp = new GeneralParameters
    implicit val iec = gp.executionContext
    implicit val its = gp.taskSupport
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
    getOrCreateResult("similarCollocations",ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      val (qlevel,termVectorQuery) = buildFinalQueryRunningSubQueries(exactCounts = false, termVectorQueryParameters.requiredQuery)
      val is = searcher(qlevel, SumScaling.ABSOLUTE)
      val ir = is.getIndexReader
      val (_,collocations) = getAggregateContextVectorForQuery(is, termVectorQuery, termVectorLocalProcessingParameters,extractContentTermsFromQuery(termVectorQuery),termVectorAggregateProcessingParameters, termVectorSamplingParameters.maxDocs)
      println("collocations: "+collocations.size)
      val futures = new ArrayBuffer[Future[LongSet]]
      val maxDocs3 = if (intermediaryTermVectorSamplingParameters.maxDocs == -1) -1 else intermediaryTermVectorSamplingParameters.maxDocs/collocations.size
      val intermediaryLimitQuery = intermediaryTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false,_)._2)
      collocations.forEach(new LongDoubleConsumer {
        override def accept(term: Long, freq: Double) {
          val termS = termOrdToTerm(ir, term)
          val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField,termS)), Occur.FILTER)
          for (q <- intermediaryLimitQuery) bqb.add(q, Occur.FILTER)
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
      val maxDocs4 = if (finalTermVectorSamplingParameters.maxDocs == -1) -1 else finalTermVectorSamplingParameters.maxDocs/collocationCollocations.size
      val finalLimitQuery = finalTermVectorLimitQueryParameters.query.map(buildFinalQueryRunningSubQueries(false,_)._2)
      val thirdOrderCollocations = for (term2 <- toParallel(termOrdsToTerms(ir, collocationCollocations))) yield {
        val bqb = new BooleanQuery.Builder().add(new TermQuery(new Term(indexMetadata.contentField,term2)), Occur.FILTER)
        for (q <- finalLimitQuery) bqb.add(q, Occur.FILTER)
        val (_,tv) = getAggregateContextVectorForQuery(is, bqb.build,finalTermVectorLocalProcessingParameters,Seq(term2), finalTermVectorAggregateProcessingParameters, maxDocs4)
        (term2,tv)
      }
      println("third order collocations:"+thirdOrderCollocations.size)
      val ordering = new Ordering[(String,Double)] {
        override def compare(x: (String,Double), y: (String,Double)) = y._2 compare x._2
      }
      val cmaxHeap = collection.mutable.PriorityQueue.empty[(String,Double)](ordering)
      val dmaxHeap = collection.mutable.PriorityQueue.empty[(String,Double)](ordering)
      var total = 0
      val termsToScores = thirdOrderCollocations.filter(!_._2.isEmpty).map(p => (p._1,Distance.cosineSimilarity(collocations,p._2,finalTermVectorDistanceCalculationParameters.filtering),Distance.diceSimilarity(collocations,p._2)))
      for ((term,cscore,dscore) <- termsToScores.seq) {
        if (cscore != 0.0 || dscore != 0.0) total+=1
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