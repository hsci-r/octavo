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
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import services.Distance
import play.api.libs.json.JsNull
import scala.collection.mutable.PriorityQueue
import com.koloboke.collect.set.hash.HashLongSets
import java.util.function.LongConsumer

@Singleton
class TermVectorDiffController @Inject() (implicit ia: IndexAccess, env: Environment) extends AQueuingController(env) {
  
  import IndexAccess._
  import ia._
  import TermVectors._
  
    // calculate distance between two term vectors across a metadata variable (use to create e.g. graphs of term meaning changes)
  def termVectorDiff() = Action { implicit request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = new GeneralParameters
    val tvq1 = new QueryParameters("t1_")
    val tvq2 = new QueryParameters("t2_")
    val tvpl = new LocalTermVectorProcessingParameters
    val tvpa = new AggregateTermVectorProcessingParameters
    val attr = p.get("attr").map(_(0)).get
    val attrLength = p.get("attrLength").map(_(0).toInt).getOrElse(-1)
    val meaningfulTerms: Int = p.get("meaningfulTerms").map(_(0).toInt).getOrElse(0)
    implicit val tlc = gp.tlc
    implicit val ec = gp.executionContext
    val qm = Json.obj("method"->"termVectorDiff","attr"->attr,"attrLength"->attrLength,"meaningfulTerms"->meaningfulTerms) ++ gp.toJson ++ tvq1.toJson ++ tvq2.toJson ++ tvpl.toJson ++ tvpa.toJson
    getOrCreateResult(qm, gp.force, gp.pretty, () => {
      val (qlevel1,termVector1Query) = buildFinalQueryRunningSubQueries(tvq1.requiredQuery)
      val (qlevel2,termVector2Query) = buildFinalQueryRunningSubQueries(tvq2.requiredQuery)
      val tvm1f = Future { getGroupedAggregateContextVectorsForQuery(searcher(qlevel1, SumScaling.ABSOLUTE), termVector1Query,tvpl,extractContentTermsFromQuery(termVector1Query),attr,attrLength,tvpa,gp.maxDocs/2) }
      val tvm2f = Future { getGroupedAggregateContextVectorsForQuery(searcher(qlevel2, SumScaling.ABSOLUTE), termVector2Query,tvpl,extractContentTermsFromQuery(termVector2Query),attr,attrLength,tvpa,gp.maxDocs/2) }
      val tvm1 = Await.result(tvm1f, Duration.Inf)
      val tvm2 = Await.result(tvm2f, Duration.Inf)
      val obj = (tvm1.keySet ++ tvm2.keySet).map(key => {
        var map = Map("attr"->Json.toJson(key),
            "distance"->(if (!tvm1.contains(key) || !tvm2.contains(key)) JsNull else {
              val distance = tvpa.distance(tvm1(key).cv,tvm2(key).cv)
              if (distance.isNaN) JsNull else Json.toJson(distance)
            }), 
            "df1"->Json.toJson(tvm1.get(key).map(_.docFreq).getOrElse(0l)),"df2"->Json.toJson(tvm2.get(key).map(_.docFreq).getOrElse(0l)),"tf1"->Json.toJson(tvm1.get(key).map(_.totalTermFreq).getOrElse(0l)),"tf2"->Json.toJson(tvm2.get(key).map(_.totalTermFreq).getOrElse(0l)))
        if (tvm1.contains(key) && tvm2.contains(key) && meaningfulTerms!=0) {
          val tv1 = tvm1(key).cv
          val tv2 = tvm2(key).cv
          Distance.normalize(tv1)
          Distance.normalize(tv2)
          val maxHeap = PriorityQueue.empty[(Long,Double)](new Ordering[(Long,Double)] {
            override def compare(x: (Long,Double), y: (Long,Double)) = y._2 compare x._2
          })
          val maxHeap1 = PriorityQueue.empty[(Long,Double)](new Ordering[(Long,Double)] {
            override def compare(x: (Long,Double), y: (Long,Double)) = y._2 compare x._2
          })
          val maxHeap2 = PriorityQueue.empty[(Long,Double)](new Ordering[(Long,Double)] {
            override def compare(x: (Long,Double), y: (Long,Double)) = y._2 compare x._2
          })
          val minHeap = PriorityQueue.empty[(Long,Double)](new Ordering[(Long,Double)] {
            override def compare(x: (Long,Double), y: (Long,Double)) = x._2 compare y._2
          })
          var total = 0
          HashLongSets.newImmutableSet(tv1.keySet, tv2.keySet).forEach(new LongConsumer() {
            override def accept(term: Long): Unit = {
              val diff = tv1.getOrDefault(term, 0.0)-tv2.getOrDefault(term, 0.0)
              val adiff = math.abs(diff)
              total+=1
              if (total<=meaningfulTerms) { 
                maxHeap += ((term,adiff))
                maxHeap1 += ((term,diff))
                maxHeap2 += ((term,-diff))
                minHeap += ((term,adiff))
              } else {
                if (maxHeap.head._2 < adiff) {
                  maxHeap.dequeue()
                  maxHeap += ((term,adiff))
                }
                if (maxHeap1.head._2 < diff) {
                  maxHeap1.dequeue()
                  maxHeap1 += ((term,diff))
                }
                if (maxHeap2.head._2 < -diff) {
                  maxHeap2.dequeue()
                  maxHeap2 += ((term,-diff))
                }
                if (minHeap.head._2 > adiff) {
                  minHeap.dequeue()
                  minHeap += ((term,adiff))
                }
              }
            }
          })
          val ir = reader(qlevel1)
          map = map + ("mostDifferentTerms"->Json.toJson(maxHeap.map(p => (termOrdToTerm(ir, p._1),p._2)).toMap))
          map = map + ("mostDistinctiveTermsForTerm1"->Json.toJson(maxHeap1.map(p => (termOrdToTerm(ir, p._1),p._2)).toMap))
          map = map + ("mostDistinctiveTermsForTerm2"->Json.toJson(maxHeap2.map(p => (termOrdToTerm(ir, p._1),p._2)).toMap))
          map = map + ("mostSimilarTerms"->Json.toJson(minHeap.map(p => (termOrdToTerm(ir, p._1),p._2)).toMap))
        }
        Json.toJson(map)
      })
      Json.toJson(obj)
    })
  }
}