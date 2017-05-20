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
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.index.DocValues
import org.apache.lucene.index.NumericDocValues

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
  
  private def getStats(is: IndexSearcher, q: Query, grouper: Option[Script], attrs: Seq[String], attrLengths: Seq[Int], attrTransformer: Option[Script], gatherTermFreqsPerDoc: Boolean)(implicit ia: IndexAccess, tlc: ThreadLocal[TimeLimitingCollector]): JsValue = {
    if (!attrs.isEmpty) {
      val matchAll = q.isInstanceOf[MatchAllDocsQuery]
      var attrGetters: Seq[(Int) => String] = null
      val groupedStats = new HashMap[Seq[String],Stats]
      val gs = new Stats
      var contentTokensNDV: NumericDocValues = null
      tlc.get.setCollector(new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) = this.scorer = scorer
  
        var scorer: Scorer = null
  
        override def collect(doc: Int) {
          val s = groupedStats.getOrElseUpdate(grouper.map(ap => {
            ap.invokeMethod("group", doc).asInstanceOf[Seq[String]]
          }).getOrElse(attrTransformer.map(ap => {
            ap.getBinding.setProperty("attrs", attrGetters.map(_(doc)))
            ap.run().asInstanceOf[Seq[String]]
          }).getOrElse(if (attrLengths.isEmpty) attrGetters.map(_(doc)) else attrGetters.zip(attrLengths).map(p => p._1(doc).substring(0,p._2)))), new Stats)
          s.docFreq += 1
          gs.docFreq += 1
          val score = if (matchAll) contentTokensNDV.get(doc).toInt else scorer.score().toInt
          if (gatherTermFreqsPerDoc) {
            s.termFreqs += score
            gs.termFreqs += score
          }
          s.totalTermFreq += score
          gs.totalTermFreq += score
        }
        
        override def doSetNextReader(context: LeafReaderContext) = {
          grouper.foreach(_.invokeMethod("setContext",context))
          attrGetters = attrs.map(ia.indexMetadata.getter(context.reader,_).andThen(_.iterator.next))
          if (matchAll) contentTokensNDV = DocValues.getNumeric(context.reader, ia.indexMetadata.contentTokensField)
        }
      })
      is.search(q, tlc.get)
      Json.obj("general"->gs.toJson,"grouped"->groupedStats.toIterable.map(p => Json.toJson(attrs.zip(p._1.map(Json.toJson(_))).toMap ++ Map("stats" -> p._2.toJson))))
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
    val attrs = p.get("attr").getOrElse(Seq.empty)
    val attrLengths = p.get("attrLength").map(_.map(_.toInt)).getOrElse(Seq.empty)
    val attrTransformer = p.get("attrTransformer").map(_(0)).map(apScript => new GroovyShell().parse(apScript))
    val grouper = p.get("grouper").map(_(0)).map(apScript => {
      val s = new GroovyShell().parse(apScript)
      val b = s.getBinding
      b.setProperty("ia", ia)
      b.setProperty("gp", gp)
      b.setProperty("q", q)
      b.setProperty("gatherTermFreqsPerDoc", gatherTermFreqsPerDoc)
      b.setProperty("attrs", attrs)
      b.setProperty("attrLengths", attrLengths)
      b.setProperty("attrTransformer", attrTransformer)
      s
    })
    val qm = Json.obj("method"->"termStats","grouper"->p.get("grouper").map(_(0)),"attrs"->attrs,"attrLengths"->attrLengths,"attrTransformer"->p.get("attrTransformer").map(_(0))) ++ gp.toJson ++ q.toJson
    implicit val ec = gp.executionContext
    getOrCreateResult(ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      val (qlevel,query) = buildFinalQueryRunningSubQueries(q.requiredQuery)
      getStats(searcher(qlevel, SumScaling.ABSOLUTE), query, grouper, attrs, attrLengths, attrTransformer, gatherTermFreqsPerDoc)
    })
  }  
}