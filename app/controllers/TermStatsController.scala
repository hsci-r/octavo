package controllers

import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import play.api.libs.json.JsValue
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.search.Scorer
import org.apache.lucene.index.LeafReaderContext
import play.api.libs.json.Json
import play.api.mvc.Action
import javax.inject.Inject
import org.apache.lucene.queryparser.classic.QueryParser
import play.api.mvc.Controller
import javax.inject.Named
import services.IndexAccess
import parameters.SumScaling
import play.api.libs.json.JsObject

@Singleton
class TermStatsController @Inject() (ia: IndexAccess) extends Controller {
  import ia._
  
  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docs = 0l
  }
  
  private def getStats(is: IndexSearcher, q: Query, gatherTermFreqsPerDoc: Boolean): JsObject = {
    val s = new Stats
    is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = true
      
      override def setScorer(scorer: Scorer) = this.scorer = scorer

      var scorer: Scorer = null
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        s.docs += 1
        val score = scorer.score().toInt
        if (gatherTermFreqsPerDoc) s.termFreqs += score
        s.totalTermFreq += score
      }
      
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Json.obj("termFreqs"->s.termFreqs.sorted,"totalTermFreq"->s.totalTermFreq,"docs"->s.docs)
  }
  
  def stats(query: String, termFreqs : Option[String]) = Action {
    val pq = queryParsers.get.parse(query)
    val gatherTermFreqsPerDoc = termFreqs.exists(v => v=="" || v.toBoolean)
    Ok(Json.prettyPrint(Json.obj("query"->pq.toString,"results"->
      Json.toJson(ia.indexMetadata.levels.map(l => (l.id,getStats(searcher(l.id,SumScaling.ABSOLUTE),pq, gatherTermFreqsPerDoc))).toMap))))
  }  
}