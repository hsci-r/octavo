package controllers

import javax.inject.{Inject, Singleton}

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search._
import parameters._
import play.api.libs.json.{Json, _}
import play.api.{Configuration, Environment}
import services.{IndexAccess, IndexAccessProvider, LevelMetadata}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@Singleton
class QueryStatsController @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  class Stats {
    val termFreqs = new ArrayBuffer[Int]
    val fieldSums = new scala.collection.mutable.HashMap[String, Long]
    val fieldGather = new scala.collection.mutable.HashMap[String, JsValue]
    var totalTermFreq = 0l
    var docFreq = 0l

    def toJson = {
      val js = Json.obj("totalTermFreq" -> totalTermFreq, "docFreq" -> docFreq) ++ Json.toJsObject(fieldGather) ++ Json.toJsObject(fieldSums)
      if (termFreqs.nonEmpty) js ++ Json.obj("termFreqs" -> termFreqs.sorted)
      else js
    }
  }
  
  private def getStats(level: LevelMetadata, is: IndexSearcher, q: Query, grpp: GroupingParameters, fieldSums: Seq[String], gatherTermFreqsPerDoc: Boolean)(implicit ia: IndexAccess, tlc: ThreadLocal[TimeLimitingCollector]): JsValue = {
    if (grpp.isDefined) {
      val gs = new Stats
      grpp.grouper.foreach(_.invokeMethod("setParameters", Seq(level, is, q, grpp, gatherTermFreqsPerDoc, gs).toArray))
      var fieldGetters: Seq[(Int) => JsValue] = null
      var fieldVGetters: Seq[(Int) => JsValue] = null
      val groupedStats = new mutable.HashMap[JsObject,Stats]
      tlc.get.setCollector(new SimpleCollector() {
        override def needsScores: Boolean = true
        
        override def setScorer(scorer: Scorer) { this.scorer = scorer }
  
        var scorer: Scorer = _
  
        override def collect(doc: Int) {
          val s = groupedStats.getOrElseUpdate(grpp.grouper.map(ap => {
            val g = ap.invokeMethod("group", doc)
            if (g.isInstanceOf[JsObject]) g.asInstanceOf[JsObject] else {
              val gm = g.asInstanceOf[java.util.Map[String,Any]].asScala
              JsObject(gm.map(p => { if (p._2.isInstanceOf[JsValue]) p else (p._1, JsString(p._2.toString))}).asInstanceOf[collection.Map[String,JsValue]])
            }
          }).getOrElse(
            JsObject(grpp.fields.zip(
              grpp.fieldTransformer.map(ap => {
                ap.getBinding.setProperty("fields", fieldGetters.map(_(doc)).asJava)
                ap.run().asInstanceOf[java.util.List[Any]].asScala.map(v => if (v.isInstanceOf[JsValue]) v else JsString(v.asInstanceOf[String])).asInstanceOf[Seq[JsValue]]
              }).getOrElse(if (grpp.fieldLengths.isEmpty) fieldGetters.map(_(doc)) else fieldGetters.zip(grpp.fieldLengths).map(p => {
                val value = p._1(doc).toString
                JsString(value.substring(0,Math.min(p._2,value.length)))
              }))))), new Stats)
          s.docFreq += 1
          gs.docFreq += 1
          val score = scorer.score().toInt
          if (gatherTermFreqsPerDoc) {
            s.termFreqs += score
            gs.termFreqs += score
          }
          s.totalTermFreq += score
          gs.totalTermFreq += score
          for ((key,getter) <- fieldSums.zip(fieldVGetters)) {
            val v = getter(doc).asInstanceOf[JsNumber].value.toLong
            s.fieldSums(key) = s.fieldSums.getOrElse(key, 0l) + v
            gs.fieldSums(key) = gs.fieldSums.getOrElse(key, 0l) + v
          }
        }
        
        override def doSetNextReader(context: LeafReaderContext) {
          grpp.grouper.foreach(_.invokeMethod("setContext",context))
          fieldGetters = grpp.fields.map(level.fields(_).jsGetter(context.reader).andThen(_.iterator.next))
          fieldVGetters = fieldSums.map(level.fields(_).jsGetter(context.reader).andThen(_.iterator.next))
        }
      })
      is.search(q, tlc.get)
      Json.obj("general"->gs.toJson,"grouped"->groupedStats.map(p => Json.obj("fields"->p._1,"stats" -> p._2.toJson)))
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
    var fieldSums = p.get("sumFields").getOrElse(Seq.empty)
    val gatherTermFreqsPerDoc = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    implicit val qm = new QueryMetadata(Json.obj("termFreqs"->gatherTermFreqsPerDoc,"sumFields"->fieldSums))
    val gp = new GeneralParameters()
    val q = new QueryParameters()
    val grpp = new GroupingParameters()
    implicit val ec = gp.executionContext
    getOrCreateResult("queryStats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      implicit val qps = documentQueryParsers
      val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, q.requiredQuery)
      getStats(ia.indexMetadata.levelMap(qlevel),searcher(qlevel, SumScaling.ABSOLUTE), query, grpp, fieldSums, gatherTermFreqsPerDoc)
    })
  }  
}