package controllers

import javax.inject.{Inject, Singleton}

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search._
import parameters.{GeneralParameters, GroupingParameters, QueryParameters, SumScaling}
import play.api.libs.json.{JsString, JsValue, Json}
import play.api.{Configuration, Environment}
import services.{IndexAccess, IndexAccessProvider, LevelMetadata}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
  
  private def getStats(level: LevelMetadata, is: IndexSearcher, q: Query, grpp: GroupingParameters, gatherTermFreqsPerDoc: Boolean)(implicit ia: IndexAccess, tlc: ThreadLocal[TimeLimitingCollector]): JsValue = {
    if (grpp.isDefined) {
      var attrGetters: Seq[(Int) => JsValue] = null
      val groupedStats = new mutable.HashMap[Seq[JsValue],Stats]
      val gs = new Stats
      tlc.get.setCollector(new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) { this.scorer = scorer }
  
        var scorer: Scorer = _
  
        override def collect(doc: Int) {
          val s = groupedStats.getOrElseUpdate(grpp.grouper.map(ap => {
            ap.invokeMethod("group", doc).asInstanceOf[Seq[JsValue]]
          }).getOrElse(grpp.attrTransformer.map(ap => {
            ap.getBinding.setProperty("attrs", attrGetters.map(_(doc)).asJava)
            ap.run().asInstanceOf[java.util.List[Any]].asScala.map(v => if (v.isInstanceOf[JsValue]) v else JsString(v.asInstanceOf[String])).asInstanceOf[Seq[JsValue]]
          }).getOrElse(if (grpp.attrLengths.isEmpty) attrGetters.map(_(doc)) else attrGetters.zip(grpp.attrLengths).map(p => {
            val value = p._1(doc).toString
            JsString(value.substring(0,Math.min(p._2,value.length)))
          }))), new Stats)
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
          grpp.grouper.foreach(_.invokeMethod("setContext",context))
          attrGetters = grpp.attrs.map(level.fields(_).jsGetter(context.reader).andThen(_.iterator.next))
        }
      })
      is.search(q, tlc.get)
      Json.obj("general"->gs.toJson,"grouped"->groupedStats.map(p => Json.obj("attrs"->Json.toJson(grpp.attrs.zip(p._1.map(Json.toJson(_))).toMap,"stats" -> p._2.toJson))))
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
  
  def queryStats(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = GeneralParameters()
    val q = QueryParameters()
    val grpp = GroupingParameters()
    val gatherTermFreqsPerDoc = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    val qm = Json.obj("termFreqs"->gatherTermFreqsPerDoc) ++ grpp.toJson ++ gp.toJson ++ q.toJson
    implicit val ec = gp.executionContext
    getOrCreateResult("queryStats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      implicit val qps = documentQueryParsers
      val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, q.requiredQuery)
      getStats(ia.indexMetadata.levelMap(qlevel),searcher(qlevel, SumScaling.ABSOLUTE), query, grpp, gatherTermFreqsPerDoc)
    })
  }  
}