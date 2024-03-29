package controllers

import java.io.ByteArrayOutputStream
import java.util

import com.github.tototoshi.csv.CSVWriter
import javax.inject.{Inject, Singleton}
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{Scorable, ScoreMode, SimpleCollector, TotalHitCountCollector}
import parameters._
import play.api.libs.json.{Json, _}
import services.{ExtendedUnifiedHighlighter, IndexAccessProvider}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

@Singleton
class QueryStatsController @Inject() (implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  class Stats {
    val termFreqs = new ArrayBuffer[Int]
    val fieldSums = new scala.collection.mutable.HashMap[String, Long]
    //val fieldGather = new scala.collection.mutable.HashMap[String, JsValue]
    var totalTermFreq = 0L
    var docFreq = 0L

    def toJson = {
      val js = Json.obj("totalTermFreq" -> totalTermFreq, "docFreq" -> docFreq) ++ Json.toJsObject(fieldSums)
      if (termFreqs.nonEmpty) js ++ Json.obj("termFreqs" -> termFreqs.sorted)
      else js
    }
  }

  def queryStats(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val fieldSums = p.get("sumField").orElse(p.get("sumFields")).getOrElse(Seq.empty)
    val gatherTermFreqsPerDoc = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    val fullJson = Json.obj("termFreqs"->gatherTermFreqsPerDoc,"sumFields"->fieldSums)
    implicit val qm = new QueryMetadata(JsObject(fullJson.fields.filter(pa => p.contains(pa._1))), fullJson)
    val gp = new GeneralParameters()
    val q = new QueryParameters()
    val grpp = new GroupingParameters()
    val sp = new SamplingParameters()
    val rfp = new ResponseFormatParameters()
    if (rfp.responseFormat == ResponseFormat.CSV && gatherTermFreqsPerDoc) throw new IllegalArgumentException("Can't return per doc term freqs in CSV format")
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
      val level = ia.indexMetadata.levelMap(qlevel)
      val is = searcher(qlevel, QueryScoring.TF)
      var fieldVGetters: Seq[Int => Option[JsValue]] = null
      if (grpp.isDefined) {
        val highlighter: ExtendedUnifiedHighlighter = if (grpp.groupByMatch || grpp.groupByMatchTerm) grpp.highlighter(is, ia.indexMetadata.indexingAnalyzers(ia.indexMetadata.contentField),matchFullSpans = true, doNotJoinMatches = true) else null
        val globalStats = new Stats
        var count = 0
        grpp.grouper.foreach(_.invokeMethod("setParameters", Seq(level, is, query, grpp, gatherTermFreqsPerDoc, globalStats).toArray))
        var fieldGetters: Seq[Int => Option[JsValue]] = null
        val groupedStats = new mutable.HashMap[JsObject,Stats]
        tlc.get.setCollector(new SimpleCollector() {
          override def scoreMode = ScoreMode.COMPLETE

          override def setScorer(scorer: Scorable): Unit = { this.scorer = scorer }

          var scorer: Scorable = _

          override def collect(doc: Int): Unit = {
            qm.documentsProcessed += 1
            count = count + 1
            if (sp.maxDocs == -1 || count <= sp.maxDocs) {
              val baseGroupDefinition = grpp.grouper.map(ap => {
                ap.invokeMethod("group", doc) match {
                  case jsObject: JsObject => jsObject
                  case gm: util.Map[_,_] =>
                    JsObject(gm.asScala.iterator.map(p => {
                      if (p._2.isInstanceOf[JsValue]) p else (p._1, JsString(p._2.toString))
                    }).toSeq.asInstanceOf[Seq[(String, JsValue)]])
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
              val fieldSumValues = for ((key, getter) <- fieldSums.zip(fieldVGetters)) yield (key, getter(doc).map(_.asInstanceOf[JsNumber].value.toLong).getOrElse(0L))
              val passages = if (grpp.groupByMatch || grpp.groupByMatchTerm) highlighter.highlightAsPassages(ia.indexMetadata.contentField, query, Array(doc), Int.MaxValue - 1).head else null
              val handleGroupByMatch = () => if (grpp.groupByMatch)
                ExtendedUnifiedHighlighter.highlightsToStrings(passages, true).asScala.map(amatch => baseGroupDefinition ++ JsObject(Seq("match" -> grpp.matchTransformer.map(ap => {
                  ap.getBinding.setProperty("match", amatch)
                  ap.run() match {
                    case v: JsValue => v
                    case v: String => JsString(v)
                  }
                }).getOrElse(JsString(if (grpp.matchLength.isDefined) amatch.substring(0, grpp.matchLength.get) else amatch)))))
                else Iterable(baseGroupDefinition)
              val groupDefinitions: Iterable[JsObject] = if (grpp.groupByMatchTerm) passages.passages.flatMap(ap => ap.getMatchTerms.take(ap.getNumMatches)).flatMap(mt => handleGroupByMatch().map(_ ++ JsObject(Seq("term"->JsString(mt.utf8ToString))))) else handleGroupByMatch()
              for (group <- groupDefinitions) {
                val s = groupedStats.getOrElseUpdate(group, new Stats)
                s.docFreq += 1
                if (gatherTermFreqsPerDoc)
                  s.termFreqs += score
                s.totalTermFreq += score
                for ((key, v) <- fieldSumValues)
                  s.fieldSums(key) = s.fieldSums.getOrElse(key, 0L) + v
              }
              if (gatherTermFreqsPerDoc)
                globalStats.termFreqs += score
              globalStats.docFreq += 1
              globalStats.totalTermFreq += score
              for ((key, v) <- fieldSumValues)
                globalStats.fieldSums(key) = globalStats.fieldSums.getOrElse(key, 0L) + v
            }
          }

          override def doSetNextReader(context: LeafReaderContext): Unit = {
            grpp.grouper.foreach(_.invokeMethod("setContext",context))
            fieldGetters = grpp.fields.map(level.fields(_).jsGetter(context.reader))
            fieldVGetters = fieldSums.map(level.fields(_).jsGetter(context.reader))
          }
        })
        is.search(query, tlc.get)
        rfp.responseFormat match {
          case ResponseFormat.CSV =>
            val baos = new ByteArrayOutputStream()
            val w = CSVWriter.open(baos)
            w.writeRow(grpp.fields ++ fieldSums ++ Seq("docFreq","totalTermFreq"))
            for ((group,stats) <- groupedStats.toSeq.sortWith((x, y) => {
              val xf = grpp.sorts.map { case (o, _, _) => x._1.value(o) }
              val yf = grpp.sorts.map { case (o, _, _) => y._1.value(o) }
              val c = grpp.compare(xf, yf)
              if (c > 0) false else if (c < 0) true else x._2.docFreq > y._2.docFreq
            })) {
              w.writeRow(grpp.fields.map(group.value.get(_).map{
                case JsString(s) => s
                case o => o.toString
              }.getOrElse("")) ++ fieldSums.map(stats.fieldSums.getOrElse(_,"")) ++ Seq(stats.docFreq,stats.totalTermFreq))
            }
            Right(Ok(baos.toByteArray).as(TEXT))
          case ResponseFormat.JSON =>
            Left(Json.obj("general" -> (globalStats.toJson ++ Json.obj("totalDocs" -> count)), "grouped" -> groupedStats.toSeq.sortWith((x, y) => {
              val xf = grpp.sorts.map { case (o, _, _) => x._1.value(o) }
              val yf = grpp.sorts.map { case (o, _, _) => y._1.value(o) }
              val c = grpp.compare(xf, yf)
              if (c > 0) false else if (c < 0) true else x._2.docFreq > y._2.docFreq
            }).map(p => Json.obj("fields" -> p._1, "stats" -> p._2.toJson))))
        }
      } else {
        val s = new Stats
        is.search(query, new SimpleCollector() {
          override def scoreMode = ScoreMode.COMPLETE

          override def setScorer(scorer: Scorable): Unit = { this.scorer = scorer }

          var scorer: Scorable = _

          override def collect(doc: Int): Unit = {
            s.docFreq += 1
            val score = scorer.score().toInt
            if (gatherTermFreqsPerDoc) s.termFreqs += score
            s.totalTermFreq += score
            val fieldSumValues = for ((key, getter) <- fieldSums.zip(fieldVGetters)) yield (key, getter(doc).map(_.asInstanceOf[JsNumber].value.toLong).getOrElse(0L))
            for ((key, v) <- fieldSumValues)
              s.fieldSums(key) = s.fieldSums.getOrElse(key, 0L) + v

          }

          override def doSetNextReader(context: LeafReaderContext): Unit = {
            fieldVGetters = fieldSums.map(level.fields(_).jsGetter(context.reader))
          }

        })
        rfp.responseFormat match {
          case ResponseFormat.CSV =>
            val baos = new ByteArrayOutputStream()
            val w = CSVWriter.open(baos)
            w.writeRow(Seq("docFreq","totalTermFreq"))
            w.writeRow(Seq(s.docFreq,s.totalTermFreq))
            Right(Ok(baos.toByteArray).as(TEXT))
          case ResponseFormat.JSON =>
            Left(s.toJson)
        }

      }
    })
  }
}