package controllers

import javax.inject.{Inject, Singleton}

import groovy.lang.{GroovyShell, Script}
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search._
import parameters.{GeneralParameters, QueryParameters, SumScaling}
import play.api.{Configuration, Environment}
import play.api.libs.json.{JsString, JsValue, Json}
import services.{IndexAccess, IndexAccessProvider, LevelMetadata}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

@Singleton
class QueryStatsController @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docFreq = 0l
    def toJson = 
      if (termFreqs.nonEmpty) Json.obj("termFreqs"->termFreqs.sorted,"totalTermFreq"->totalTermFreq,"docFreq"->docFreq)
      else Json.obj("totalTermFreq"->totalTermFreq,"docFreq"->docFreq)
  }
  
  private def getStats(level: LevelMetadata, is: IndexSearcher, q: Query, grouper: Option[Script], attrs: Seq[String], attrLengths: Seq[Int], attrTransformer: Option[Script], gatherTermFreqsPerDoc: Boolean)(implicit ia: IndexAccess, tlc: ThreadLocal[TimeLimitingCollector]): JsValue = {
    if (attrs.nonEmpty) {
      var attrGetters: Seq[(Int) => JsValue] = null
      val groupedStats = new mutable.HashMap[Seq[JsValue],Stats]
      val gs = new Stats
      tlc.get.setCollector(new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) { this.scorer = scorer }
  
        var scorer: Scorer = _
  
        override def collect(doc: Int) {
          val s = groupedStats.getOrElseUpdate(grouper.map(ap => {
            ap.invokeMethod("group", doc).asInstanceOf[Seq[JsValue]]
          }).getOrElse(attrTransformer.map(ap => {
            ap.getBinding.setProperty("attrs", attrGetters.map(_(doc)))
            ap.run().asInstanceOf[Seq[JsValue]]
          }).getOrElse(if (attrLengths.isEmpty) attrGetters.map(_(doc)) else attrGetters.zip(attrLengths).map(p => JsString(p._1(doc).as[String].substring(0,p._2))))), new Stats)
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
        
        override def doSetNextReader(context: LeafReaderContext) {
          grouper.foreach(_.invokeMethod("setContext",context))
          attrGetters = attrs.map(level.fields(_).jsGetter(context.reader).andThen(_.iterator.next))
        }
      })
      is.search(q, tlc.get)
      Json.obj("general"->gs.toJson,"grouped"->groupedStats.map(p => Json.obj("attrs"->Json.toJson(attrs.zip(p._1.map(Json.toJson(_))).toMap,"stats" -> p._2.toJson))))
    } else {
      val s = new Stats
      is.search(q, new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) { this.scorer = scorer }
  
        var scorer: Scorer = _
  
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
    val gatherTermFreqsPerDoc = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    val attrs = p.getOrElse("attr", Seq.empty)
    val attrLengths = p.getOrElse("attrLength", Seq.empty).map(_.toInt)
    val attrTransformer = p.get("attrTransformer").map(_.head).map(apScript => new GroovyShell().parse(apScript))
    val grouper = p.get("grouper").map(_.head).map(apScript => {
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
    val qm = Json.obj("grouper"->p.get("grouper").map(_.head),"attrs"->attrs,"attrLengths"->attrLengths,"attrTransformer"->p.get("attrTransformer").map(_.head)) ++ gp.toJson ++ q.toJson
    implicit val ec = gp.executionContext
    getOrCreateResult("queryStats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      implicit val qps = documentQueryParsers
      val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, q.requiredQuery)
      getStats(ia.indexMetadata.levelMap(qlevel),searcher(qlevel, SumScaling.ABSOLUTE), query, grouper, attrs, attrLengths, attrTransformer, gatherTermFreqsPerDoc)
    })
  }  
}