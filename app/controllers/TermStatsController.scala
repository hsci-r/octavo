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
import parameters.QueryParameters
import akka.stream.Materializer
import play.api.Environment
import parameters.GeneralParameters
import scala.collection.mutable.HashMap
import services.IndexAccessProvider
import play.api.Configuration
import groovy.lang.GroovyShell
import groovy.lang.Script
import org.apache.lucene.search.TimeLimitingCollector

@Singleton
class TermStatsController @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docFreq = 0l
    def toJson = 
      if (!termFreqs.isEmpty) Json.obj("termFreqs"->termFreqs.sorted,"totalTermFreq"->totalTermFreq,"docFreq"->docFreq)
      else Json.obj("totalTermFreq"->totalTermFreq,"docFreq"->docFreq)
  }
  
  private def getStats(is: IndexSearcher, q: Query, attrO: Option[String], attrLength: Int, attrProcessor: Option[Script], gatherTermFreqsPerDoc: Boolean)(implicit ia: IndexAccess, tlc: ThreadLocal[TimeLimitingCollector]): JsValue = {
    if (attrO.isDefined) {
      val attr = attrO.get
      var attrGetter: (Int) => String = null
      val groupedStats = new HashMap[String,Stats]
      val gs = new Stats
      tlc.get.setCollector(new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) = this.scorer = scorer
  
        var scorer: Scorer = null
  
        override def collect(doc: Int) {
          val cattr = attrGetter(doc)
          val s = groupedStats.getOrElseUpdate(attrProcessor.map(ap => {
            ap.getBinding.setProperty("attr", cattr)
            ap.run().asInstanceOf[String]
          }).getOrElse(if (attrLength == -1) cattr else cattr.substring(0,attrLength)), new Stats)
          s.docFreq += 1
          gs.docFreq += 1
          val score = scorer.score().toInt
          if (gatherTermFreqsPerDoc) {
            s.termFreqs += score
            gs.termFreqs += score
          }
          s.totalTermFreq += score
          gs.totalTermFreq += score
        }
        
        override def doSetNextReader(context: LeafReaderContext) = {
          attrGetter = ia.indexMetadata.getter(context.reader, attr).andThen(_.iterator.next)
        }
      })
      is.search(q, tlc.get)
      Json.obj("general"->gs.toJson,"grouped"->groupedStats.toIterable.map(p => Json.obj("attr"->p._1,"stats"->p._2.toJson)))
    } else {
      val s = new Stats
      is.search(q, new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) = this.scorer = scorer
  
        var scorer: Scorer = null
  
        override def collect(doc: Int) {
          s.docFreq += 1
          val score = scorer.score().toInt
          if (gatherTermFreqsPerDoc) s.termFreqs += score
          s.totalTermFreq += score
        }
        
      })
      s.toJson
    }
  }
  
  def stats(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = new GeneralParameters
    val q = new QueryParameters
    val gatherTermFreqsPerDoc = p.get("termFreqs").exists(v => v(0)=="" || v(0).toBoolean)
    val attr = p.get("attr").map(_(0))
    val attrLength = p.get("attrLength").map(_(0).toInt).getOrElse(-1)
    val attrProcessor = p.get("attrProcessor").map(_(0)).map(apScript => new GroovyShell().parse(apScript))
    val qm = Json.obj("method"->"termStats","attr"->attr,"attrLength"->attrLength) ++ gp.toJson ++ q.toJson
    implicit val ec = gp.executionContext
    getOrCreateResult(ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      val (qlevel,query) = buildFinalQueryRunningSubQueries(q.requiredQuery)
      getStats(searcher(qlevel, SumScaling.ABSOLUTE), query, attr, attrLength, attrProcessor, gatherTermFreqsPerDoc)
    })
  }  
}