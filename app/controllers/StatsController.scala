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
import parameters.Level

@Singleton
class StatsController @Inject() (ia: IndexAccess) extends Controller {
  import ia._
  
  import IndexAccess.queryParsers

  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docs = 0l
  }
  
  private def getStats(is: IndexSearcher, q: Query, gatherTermFreqsPerDoc: Boolean): JsValue = {
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
    Json.toJson(Map("termFreqs"->Json.toJson(s.termFreqs.sorted),"totalTermFreq"->Json.toJson(s.totalTermFreq),"docs"->Json.toJson(s.docs)))
  }
  
  def stats(query: String, termFreqs : Option[String]) = Action {
    val pq = queryParsers.get.parse(query)
    val gatherTermFreqsPerDoc = termFreqs.exists(v => v=="" || v.toBoolean)
    Ok(Json.prettyPrint(Json.toJson(Map("query"->Json.toJson(pq.toString),"results"->Json.toJson(Map("document"->getStats(searcher(Level.DOCUMENT,SumScaling.ABSOLUTE),pq, gatherTermFreqsPerDoc),"documentpart"->getStats(searcher(Level.DOCUMENTPART,SumScaling.ABSOLUTE),pq, gatherTermFreqsPerDoc),"paragraph"->getStats(searcher(Level.PARAGRAPH, SumScaling.ABSOLUTE),pq, gatherTermFreqsPerDoc),"section"->getStats(searcher(Level.SECTION, SumScaling.ABSOLUTE),pq, gatherTermFreqsPerDoc)))))))
  }  
}