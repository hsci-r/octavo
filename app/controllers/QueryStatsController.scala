package controllers

import java.util

import javax.inject.{Inject, Singleton}
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search._
import parameters._
import play.api.libs.json.{Json, _}
import services.{ExtendedUnifiedHighlighter, IndexAccess, IndexAccessProvider, LevelMetadata}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@Singleton
class QueryStatsController @Inject() (implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
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
  
  private def getStats(level: LevelMetadata, is: IndexSearcher, q: Query, grpp: GroupingParameters, sp: SamplingParameters, fieldSums: Seq[String], gatherTermFreqsPerDoc: Boolean)(implicit ia: IndexAccess, tlc: ThreadLocal[TimeLimitingCollector], qm: QueryMetadata): JsValue = {
    if (grpp.isDefined) {
      val highlighter = if (grpp.groupByMatch) grpp.highlighter(is, ia.indexMetadata.indexingAnalyzers(ia.indexMetadata.contentField)) else null
      val globalStats = new Stats
      var count = 0
      grpp.grouper.foreach(_.invokeMethod("setParameters", Seq(level, is, q, grpp, gatherTermFreqsPerDoc, globalStats).toArray))
      var fieldGetters: Seq[Int => Option[JsValue]] = null
      var fieldVGetters: Seq[Int => Option[JsValue]] = null
      val groupedStats = new mutable.HashMap[JsObject,Stats]
      tlc.get.setCollector(new SimpleCollector() {
        override def scoreMode = ScoreMode.COMPLETE

        override def setScorer(scorer: Scorable) { this.scorer = scorer }

        var scorer: Scorable = _

        override def collect(doc: Int): Unit = {
          qm.documentsProcessed += 1
          count = count + 1
          if (sp.maxDocs == -1 || count <= sp.maxDocs) {
            val baseGroupDefinition = grpp.grouper.map(ap => {
              ap.invokeMethod("group", doc) match {
                case jsObject: JsObject => jsObject
                case gm: util.Map[_,_] =>
                  JsObject(gm.asScala.map(p => {
                    if (p._2.isInstanceOf[JsValue]) p else (p._1, JsString(p._2.toString))
                  }).asInstanceOf[collection.Map[String, JsValue]])
              }
            }).getOrElse(
              JsObject(grpp.fields.zip(
                grpp.fieldTransformer.map(ap => {
                  ap.getBinding.setProperty("fields", fieldGetters.map(_ (doc).orNull).asJava)
                  ap.run().asInstanceOf[java.util.List[Any]].asScala.map(v => if (v.isInstanceOf[JsValue]) v else JsString(v.asInstanceOf[String])).asInstanceOf[Seq[JsValue]]
                }).getOrElse(if (grpp.fieldLengths.isEmpty) fieldGetters.map(_ (doc).getOrElse(JsNull)) else fieldGetters.zip(grpp.fieldLengths).map(p => {
                  val value = p._1(doc).map {
                    case JsString(s) => s
                    case a => a.toString
                  }.getOrElse("")
                  if (p._2 == -1) JsString(value) else JsString(value.substring(0, Math.min(p._2, value.length)))
                })))))
            val score = scorer.score().toInt
            val fieldSumValues = for ((key, getter) <- fieldSums.zip(fieldVGetters)) yield (key, getter(doc).map(_.asInstanceOf[JsNumber].value.toLong).getOrElse(0l))
            val groupDefinitions: Iterable[JsObject] = if (grpp.groupByMatch)
              ExtendedUnifiedHighlighter.highlightsToStrings(highlighter.highlight(ia.indexMetadata.contentField, q, Array(doc), Int.MaxValue - 1).head, true).asScala.map(amatch => baseGroupDefinition ++ JsObject(Seq("match" -> grpp.matchTransformer.map(ap => {
                ap.getBinding.setProperty("match", amatch)
                ap.run() match {
                  case v: JsValue => v
                  case v: String => JsString(v)
                }
              }).getOrElse(JsString(if (grpp.matchLength.isDefined) amatch.substring(0, grpp.matchLength.get) else amatch)))))
            else Iterable(baseGroupDefinition)
            for (group <- groupDefinitions) {
              val s = groupedStats.getOrElseUpdate(group, new Stats)
              s.docFreq += 1
              if (gatherTermFreqsPerDoc)
                s.termFreqs += score
              s.totalTermFreq += score
              for ((key, v) <- fieldSumValues)
                s.fieldSums(key) = s.fieldSums.getOrElse(key, 0l) + v
            }
            if (gatherTermFreqsPerDoc)
              globalStats.termFreqs += score
            globalStats.docFreq += 1
            globalStats.totalTermFreq += score
            for ((key, v) <- fieldSumValues)
              globalStats.fieldSums(key) = globalStats.fieldSums.getOrElse(key, 0l) + v
          }
        }

        override def doSetNextReader(context: LeafReaderContext) {
          grpp.grouper.foreach(_.invokeMethod("setContext",context))
          fieldGetters = grpp.fields.map(level.fields(_).jsGetter(context.reader))
          fieldVGetters = fieldSums.map(level.fields(_).jsGetter(context.reader))
        }
      })
      is.search(q, tlc.get)
      Json.obj("general"->(globalStats.toJson ++ Json.obj("totalDocs"->count)),"grouped"->groupedStats.toSeq.sortWith((x,y) => {
        val xf = grpp.sorts.map{ case (o,_,_) => x._1.value(o)}
        val yf = grpp.sorts.map{ case (o,_,_) => y._1.value(o)}
        val c = grpp.compare(xf,yf)
        if (c>0) false else if (c<0) true else x._2.docFreq > y._2.docFreq
      }).map(p => Json.obj("fields"->p._1,"stats" -> p._2.toJson)))
    } else {
      val s = new Stats
      is.search(q, new SimpleCollector() {
        override def scoreMode = ScoreMode.COMPLETE
        
        override def setScorer(scorer: Scorable) { this.scorer = scorer }
  
        var scorer: Scorable = _
  
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
    val fieldSums = p.get("sumField").orElse(p.get("sumFields")).getOrElse(Seq.empty)
    val gatherTermFreqsPerDoc = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    implicit val qm = new QueryMetadata(Json.obj("termFreqs"->gatherTermFreqsPerDoc,"sumFields"->fieldSums))
    val gp = new GeneralParameters()
    val q = new QueryParameters()
    val grpp = new GroupingParameters()
    val sp = new SamplingParameters()
    implicit val ec = gp.executionContext
    implicit val tlc = gp.tlc
    implicit val qps = documentQueryParsers
    getOrCreateResult("queryStats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      val (qlevel, query) = buildFinalQueryRunningSubQueries(exactCounts = false, q.requiredQuery)
      val hc = new TotalHitCountCollector()
      gp.etlc.setCollector(hc)
      searcher(qlevel, QueryScoring.NONE).search(query, gp.etlc)
      qm.estimatedDocumentsToProcess = if (sp.maxDocs == -1) hc.getTotalHits else Math.min(hc.getTotalHits, sp.maxDocs)
      qm.estimatedNumberOfResults = if (grpp.limit != -1) grpp.limit else Math.min(hc.getTotalHits, grpp.limit)
    }, () => {
      val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, q.requiredQuery)
      Left(getStats(ia.indexMetadata.levelMap(qlevel),searcher(qlevel, QueryScoring.TF), query, grpp, sp, fieldSums, gatherTermFreqsPerDoc))
    })
  }
}