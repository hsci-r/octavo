package controllers

import java.io.ByteArrayOutputStream

import com.github.tototoshi.csv.CSVWriter
import com.koloboke.collect.map.LongDoubleMap
import com.koloboke.collect.map.hash.HashIntObjMaps
import javax.inject._
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{Scorable, ScoreMode, SimpleCollector, TotalHitCountCollector}
import org.apache.lucene.util.PriorityQueue
import parameters._
import play.api.libs.json._
import play.api.mvc.{Action, AnyContent}
import services.ExtendedUnifiedHighlighter.Passages
import services.{ExtendedUnifiedHighlighter, IndexAccessProvider, TermVectors}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


@Singleton
class SearchController @Inject() (iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  import TermVectors._

  def search(index: String): Action[AnyContent] = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val termVectors = p.get("termVectors").exists(v => v.head=="" || v.head.toBoolean)
    /** minimum query score (by default term match frequency) for doc to be included in query results */
    val minScore: Float = p.get("minScore").map(_.head.toFloat).getOrElse(0.0f)
    val maxScore: Float = p.get("maxScore").map(_.head.toFloat).getOrElse(-1.0f)
    val fullJson = Json.obj(
      "minScore"->minScore,
      "maxScore"->maxScore,
      "termVectors"->termVectors
    )
    implicit val qm = new QueryMetadata(JsObject(fullJson.fields.filter(pa => p.get(pa._1).isDefined)),fullJson)
    val qp = new QueryParameters()
    val gp = new GeneralParameters()
    val srp = new QueryReturnParameters()
    val ctv = new QueryParameters("ctv_")
    val ctvs = new SamplingParameters("ctv_")
    val ctvpl = new LocalTermVectorProcessingParameters("ctv_")
    val ctvpa = new AggregateTermVectorProcessingParameters("ctv_")
    val ctvdp = new TermVectorDistanceCalculationParameters("ctv_")
    val rtvpl = new LocalTermVectorProcessingParameters("rtv_")
    val rtvpa = new AggregateTermVectorProcessingParameters("rtv_")
    val rtvl = new LimitParameters("rtv_")
    val rtvdr = new TermVectorDimensionalityReductionParameters("rtv_")
    implicit val iec = gp.executionContext
    implicit val tlc = gp.tlc
    implicit val ifjp = gp.forkJoinPool
    getOrCreateResult("search",ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      val qhits = {
        val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = false, qp.requiredQuery)
        val hc = new TotalHitCountCollector()
        gp.etlc.setCollector(hc)
        searcher(qlevel, QueryScoring.NONE).search(query, gp.etlc)
        hc.getTotalHits
      }
      val chits = if (ctv.query.isDefined) {
        val (qlevel, query) = buildFinalQueryRunningSubQueries(exactCounts = false, ctv.requiredQuery)
        val hc = new TotalHitCountCollector()
        gp.etlc.setCollector(hc)
        searcher(qlevel, QueryScoring.NONE).search(query, gp.etlc)
        hc.getTotalHits
      } else 0
      qm.estimatedDocumentsToProcess = qhits + chits
      qm.estimatedNumberOfResults = Math.min(qhits, srp.limit)
    }, () => {
      val (queryLevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, qp.requiredQuery)
      val ql = ia.indexMetadata.levelMap(queryLevel)
      val fieldsIncludeOffsetData = srp.fields.contains("offsetData")
      val rfields = {
        val f =  srp.fields.filter(p => p != "offsetData") ++ srp.sorts.filter(p => p._1 != "score" && p._1 != "offsetData" && !srp.fields.contains(p._1)).map(_._1)
        if (srp.offsetData && ql.fields.contains("startOffset") && !f.contains("startOffset")) f :+ "startOffset" else f
      }
      val is = searcher(queryLevel,srp.queryScoring)
      val ir = reader(queryLevel)
      val it = termsEnums(queryLevel).get
      var total = 0
      var totalScore = 0.0
      val mcompare : ((Int,Float,Seq[JsValue]),(Int,Float,Seq[JsValue])) => Int = if (srp.sorts.nonEmpty) (x,y) => srp.compare(x._3,y._3) match {
        case 0 => x._1.compare(y._1)
        case o => o
      } else (x,y) => y._2.compare(x._2) match {
        case 0 => x._1.compare(y._1)
        case o => o
      }
      var responseSizeHint = 0L
      val nlc = if (srp.limit == -1) new ArrayBuffer[(Int,Float,Seq[JsValue])] else null
      val maxHeap: PriorityQueue[(Int, Float, Seq[JsValue])] = if (srp.limit == -1) null else new PriorityQueue[(Int,Float,Seq[JsValue])](srp.offset + srp.limit) {
        override def lessThan(a: (Int, Float, Seq[JsValue]), b: (Int, Float, Seq[JsValue])): Boolean = mcompare(b,a)<0
      }//mutable.PriorityQueue.empty[(Int,Float,Seq[JsValue])]((x,y) => mcompare(x,y))
      val compareTermVector = if (ctv.query.isDefined)
        getAggregateContextVectorForQuery(is, it, buildFinalQueryRunningSubQueries(exactCounts = false, ctv.query.get)._2,ctvpl, extractContentTermBytesRefsFromQuery(query),ctvpa, ctvs.maxDocs) else null
      val we = if (srp.returnExplanations)
        query.createWeight(is, ScoreMode.COMPLETE, 1.0f)
      else null
      val processDocFields = (context: LeafReaderContext, doc: Int, getters: Map[String,Int => Option[JsValue]]) => {
        val fields = new mutable.HashMap[String, JsValue]
        for (field <- rfields;
             value <- getters(field)(doc)) {
          value match {
            case value: JsString =>
              responseSizeHint += value.value.length
              if (responseSizeHint>gp.maxResponseSize) throw new ResponseTooBigException(responseSizeHint)
            case _ =>
          }
          fields += (field -> value)
        }
        val cv = if (termVectors || ctv.query.isDefined) getTermVectorForDocument(ir, it, doc, rtvpl, rtvpa) else null
        if (fieldsIncludeOffsetData)
          fields += ("offsetData" -> Json.toJson(ia.offsetDataIterator(doc).map(p => Json.obj("offset"->p._1,"data"->p._2)).toArray))
        if (ctv.query.isDefined)
          fields += ("distance" -> Json.toJson(ctvdp.distance(cv, compareTermVector._2)))
        if (srp.returnExplanations)
          fields += ("explanation" -> Json.toJson(we.explain(context, doc).toString))
        if (termVectors && rtvdr.dimensions == 0) fields += ("termVector" -> Json.toJson(termOrdMapToOrderedStringIterable(it, limitTermVector(cv, rtvl)).map(p => Json.obj("term" -> p._1, "weight" -> p._2))))
        (fields, if (rtvdr.dimensions >0) cv else null)
      }
      val docFields = HashIntObjMaps.getDefaultFactory[collection.Map[String,JsValue]]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[collection.Map[String,JsValue]]
      val docVectorsForMDS = if (rtvdr.dimensions>0) HashIntObjMaps.getDefaultFactory[LongDoubleMap]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[LongDoubleMap] else null
      val collector = new SimpleCollector() {

        override def scoreMode = ScoreMode.COMPLETE
        var scorer: Scorable = _
        var context: LeafReaderContext = _

        var getters: Map[String,Int => Option[JsValue]] = _
        var sortGetters: Seq[Int => Option[JsValue]] = Seq.empty

        override def setScorer(scorer: Scorable): Unit = {this.scorer=scorer}

        override def collect(ldoc: Int): Unit = {
          qm.documentsProcessed += 1
          val doc = context.docBase + ldoc
          if (scorer.score >= minScore && (maxScore == -1.0f || scorer.score<=maxScore)) {
            total+=1
            totalScore += scorer.score
            if (srp.limit == -1) {
              val (cdocFields, cdocVectors) = processDocFields(context, doc, getters)
              docFields.put(doc, cdocFields)
              if (cdocVectors != null) docVectorsForMDS.put(doc, cdocVectors)
              nlc += ((doc, scorer.score, srp.sorts.map(p => if (p._1 == "score") JsNumber(BigDecimal.decimal(scorer.score)) else cdocFields(p._1))))
            } else maxHeap.insertWithOverflow((doc, scorer.score, sortGetters.map(sg => sg(doc).orNull))) /*if (total<=srp.offset + srp.limit)
              maxHeap += ((doc, scorer.score, sortGetters.map(sg => sg(doc).orNull)))
            else {
              maxHeap
              val entry = (doc, scorer.score, sortGetters.map(sg => sg(doc).orNull))
              if (mcompare(maxHeap.head,entry)>0) {
                maxHeap.dequeue()
                maxHeap += entry
              }
            }*/
          }
        }

        override def doSetNextReader(context: LeafReaderContext): Unit = {
          this.context = context
          if (srp.limit == -1)
            this.getters = rfields.map(f => f -> ql.fields(f).jsGetter(context.reader)).toMap
          else if (srp.sorts.nonEmpty)
            this.sortGetters = srp.sorts.map(f => if (f._1 == "score") (_: Int) => Some(JsNumber(BigDecimal.decimal(scorer.score))) else ql.fields(f._1).jsGetter(context.reader))
        }
      }
      tlc.get.setCollector(collector)
      is.search(query, gp.tlc.get)
      val values: mutable.Seq[(Int,Float,Seq[JsValue])] = if (srp.limit == -1) nlc.sortInPlace()((a,b) => mcompare(a,b)).drop(srp.offset) else {
        val v = new Array[(Int,Float,Seq[JsValue])](math.max(math.min(maxHeap.size-srp.offset,srp.limit),0))
        for (i <- v.indices.reverse) v(i)=maxHeap.pop()
        v
      }
      if (srp.limit!= -1) {
        val lr = ir.leaves.get(0)
        val jsGetters = rfields.map(f => f -> ql.fields.getOrElse(f, throw new IllegalArgumentException("Tried to request unknown field: " + f)).jsGetter(lr.reader)).toMap
        values.sortBy(_._1).foreach{p => // sort by id so that advanceExact works
          val doc = p._1 - lr.docBase
          val (cdocFields, cdocVectors) = processDocFields(lr, doc, jsGetters)
          docFields.put(p._1, cdocFields)
          if (cdocVectors != null) docVectorsForMDS.put(p._1, limitTermVector(cdocVectors,rtvl))
        }
      }
      val cvs = if (rtvdr.dimensions > 0) {
        val nonEmptyVectorsAndTheirOriginalIndices = values.map(p => docVectorsForMDS.get(p._1)).zipWithIndex.filter(p => !p._1.isEmpty)
        val mdsValues = rtvdr.dimensionalityReduction(nonEmptyVectorsAndTheirOriginalIndices.map(p => p._1), rtvdr).map(Json.toJson(_)).toSeq
        val originalIndicesToMDSValueIndices = nonEmptyVectorsAndTheirOriginalIndices.map(_._2).zipWithIndex.toMap
        values.indices.map(i => originalIndicesToMDSValueIndices.get(i).map(vi => Json.toJson(mdsValues(vi))).getOrElse(JsNull))
      } else null
      val matchesByDocs: Array[Passages] = if (srp.snippetLimit!=0) {
        val highlighter: ExtendedUnifiedHighlighter = srp.highlighter(is, indexMetadata.indexingAnalyzers(indexMetadata.contentField),matchFullSpans = false, doNotJoinMatches = false)
        highlighter.highlightAsPassages(indexMetadata.contentField, query, values.map(_._1).toArray, if (srp.snippetLimit == -1) Int.MaxValue - 1 else srp.snippetLimit)
      } else null
      srp.responseFormat match {
        case ResponseFormat.CSV =>
          val baos = new ByteArrayOutputStream()
          val w = CSVWriter.open(baos)
          w.writeRow(Seq("score") ++ srp.fields)
          for ((doc,score,_) <- values) {
            val df = docFields.get(doc)
            w.writeRow(Seq(""+(if (srp.queryScoring != QueryScoring.TF) score else score.toInt)) ++ srp.fields.map(df.get(_).map{
              case JsString(s) => s
              case o => o.toString
            }.getOrElse("")))
          }
          w.close()
          Right(Ok(baos.toByteArray).as(TEXT))
        case ResponseFormat.JSON =>
          Left(Json.obj(
            "final_query"->{
              val qs = query.toString
              if (qs.length <= 500) qs else qs.substring(0,497) + "..."
            },
            "total"->total,
            "totalScore"->(if (srp.queryScoring != QueryScoring.TF) Json.toJson(totalScore) else Json.toJson(totalScore.toInt)),
            "docs"->values.zipWithIndex.map{
              case ((doc,score,_),i) =>
                var df = docFields.get(doc)
                if (cvs!=null) df = df ++ Map("termVector"->cvs(i))
                if (srp.snippetLimit!=0) {
                  val g = ia.offsetDataGetter
                  df = df ++ Map("snippets" -> Json.toJson(Option(matchesByDocs(i)).map(hl =>
                    (if (srp.snippetLimit != -1) hl.passages.take(srp.snippetLimit) else hl.passages).map(p => {
                      val matches = new mutable.HashMap[(Int, Int), ArrayBuffer[String]]
                      var i = 0
                      while (i < p.getNumMatches) {
                        matches.getOrElseUpdate((p.getMatchStarts()(i), p.getMatchEnds()(i)), new ArrayBuffer[String]()) += p.getMatchTerms()(i).utf8ToString
                        i += 1
                      }
                      val highlightedSnippet = ExtendedUnifiedHighlighter.highlightToString(p, hl.content)
                      responseSizeHint += highlightedSnippet.length
                      if (responseSizeHint>gp.maxResponseSize) throw new ResponseTooBigException(responseSizeHint)
                      var md =
                        Json.obj(

                          "start" -> p.getStartOffset,
                          "end" -> p.getEndOffset,
                          "matches" -> matches.keys.map(k => {
                            var m = Json.obj("text"->hl.content.substring(k._1,k._2),"start" -> k._1, "end" -> k._2, "terms" -> matches(k))
                            if (srp.offsetData) m = m ++ Json.obj("data"-> g(doc,k._1,srp.matchOffsetSearchType))
                            m
                          }),
                          "snippet" -> highlightedSnippet
                        )
                      if (srp.offsetData) md = md ++ Json.obj(
                        "startData" -> g(doc, p.getStartOffset, srp.startOffsetSearchType),
                        "endData" -> g(doc, p.getEndOffset, srp.endOffsetSearchType)
                      )
                      md
                    })
                  ).getOrElse(Array.empty)))
                }
                df ++ Map("score" -> (if (srp.queryScoring != QueryScoring.TF) Json.toJson(score) else Json.toJson(score.toInt)))
            }))
      }
    })
  }
 
}
