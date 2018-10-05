package controllers

import com.koloboke.collect.map.LongDoubleMap
import com.koloboke.collect.map.hash.HashIntObjMaps
import javax.inject._
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{Scorer, SimpleCollector, TotalHitCountCollector}
import parameters._
import play.api.libs.json.{JsNull, JsValue, Json}
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
    implicit val qm = new QueryMetadata(Json.obj(
      "minScore"->minScore,
      "maxScore"->maxScore,
      "termVectors"->termVectors
    ))
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
        val hc = new TotalHitCountCollector()
        val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = false, qp.requiredQuery)
        searcher(qlevel, SumScaling.ABSOLUTE).search(query, hc)
        hc.getTotalHits
      }
      val chits = if (ctv.query.isDefined) {
        val hc = new TotalHitCountCollector()
        val (qlevel, query) = buildFinalQueryRunningSubQueries(exactCounts = false, ctv.requiredQuery)
        searcher(qlevel, SumScaling.ABSOLUTE).search(query, hc)
        hc.getTotalHits
      } else 0
      qm.estimatedDocumentsToProcess = qhits + chits
      qm.estimatedNumberOfResults = Math.min(qhits, srp.limit)
    }, () => {
      val (queryLevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, qp.requiredQuery)
      val ql = ia.indexMetadata.levelMap(queryLevel)
      val rfields = if (srp.offsetData && ql.fields.contains("startOffset") && !srp.fields.contains("startOffset")) srp.fields :+ "startOffset" else srp.fields
      val is = searcher(queryLevel,srp.sumScaling)
      val ir = is.getIndexReader
      var total = 0
      var totalScore = 0.0
      val maxHeap = mutable.PriorityQueue.empty[(Int,Float)]((x: (Int, Float), y: (Int, Float)) => y._2 compare x._2)
      val compareTermVector = if (ctv.query.isDefined)
        getAggregateContextVectorForQuery(is, buildFinalQueryRunningSubQueries(exactCounts = false, ctv.query.get)._2,ctvpl, extractContentTermsFromQuery(query),ctvpa, ctvs.maxDocs) else null
      val we = if (srp.returnExplanations)
        query.createWeight(is, true, 1.0f)
      else null
      val processDocFields = (context: LeafReaderContext, doc: Int, getters: Map[String,Int => Option[JsValue]]) => {
        val fields = new mutable.HashMap[String, JsValue]
        for (field <- rfields;
             value <- getters(field)(doc)) fields += (field -> value)
        val cv = if (termVectors || ctv.query.isDefined) getTermVectorForDocument(ir, doc, rtvpl, rtvpa) else null
        if (ctv.query.isDefined)
          fields += ("distance" -> Json.toJson(ctvdp.distance(cv, compareTermVector._2)))
        if (srp.returnExplanations)
          fields += ("explanation" -> Json.toJson(we.explain(context, doc).toString))
        if (termVectors && rtvdr.dimensions == 0) fields += ("termVector" -> Json.toJson(termOrdMapToOrderedTermSeq(ir, limitTermVector(cv, rtvl)).map(p => Json.obj("term" -> p._1, "weight" -> p._2))))
        (fields, if (rtvdr.dimensions >0) cv else null)
      }
      val docFields = HashIntObjMaps.getDefaultFactory[collection.Map[String,JsValue]]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[collection.Map[String,JsValue]]
      val docVectorsForMDS = if (rtvdr.dimensions>0) HashIntObjMaps.getDefaultFactory[LongDoubleMap]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[LongDoubleMap] else null
      val collector = new SimpleCollector() {

        override def needsScores: Boolean = true
        var scorer: Scorer = _
        var context: LeafReaderContext = _

        var getters: Map[String,Int => Option[JsValue]] = _

        override def setScorer(scorer: Scorer) {this.scorer=scorer}

        override def collect(ldoc: Int) {
          qm.documentsProcessed += 1
          val doc = context.docBase + ldoc
          if (scorer.score >= minScore && (maxScore == -1.0f || scorer.score<=maxScore)) {
            total+=1
            totalScore += scorer.score
            if (srp.limit == -1) {
              val (cdocFields, cdocVectors) = processDocFields(context, doc, getters)
              docFields.put(doc, cdocFields)
              if (cdocVectors != null) docVectorsForMDS.put(doc, cdocVectors)
              maxHeap += ((doc, scorer.score))
            } else if (total<=srp.offset + srp.limit)
              maxHeap += ((doc, scorer.score))
            else if (maxHeap.head._2<scorer.score) {
              maxHeap.dequeue()
              maxHeap += ((doc, scorer.score))
            }
          }
        }

        override def doSetNextReader(context: LeafReaderContext) {
          this.context = context
          if (srp.limit == -1)
            this.getters = rfields.map(f => f -> ql.fields(f).jsGetter(context.reader)).toMap
        }
      }
      tlc.get.setCollector(collector)
      is.search(query, gp.tlc.get)
      val values = maxHeap.dequeueAll.reverse.drop(srp.offset)
      if (srp.limit!= -1) {
        val lr = ir.leaves.get(0)
        val jsGetters = rfields.map(f => f -> ql.fields(f).jsGetter(lr.reader)).toMap
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
        val highlighter = srp.highlighter(is, indexMetadata.indexingAnalyzers(indexMetadata.contentField))
        highlighter.highlight(indexMetadata.contentField, query, values.map(_._1).toArray, if (srp.snippetLimit == -1) Int.MaxValue - 1 else srp.snippetLimit)
      } else null
      Left(Json.obj(
        "final_query"->query.toString,
        "total"->total,
        "totalScore"->(if (srp.sumScaling == SumScaling.DF) Json.toJson(totalScore) else Json.toJson(totalScore.toInt)),
        "docs"->values.zipWithIndex.map{
          case ((doc,score),i) =>
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
                  var md =
                    Json.obj(

                      "start" -> p.getStartOffset,
                      "end" -> p.getEndOffset,
                      "matches" -> matches.keys.map(k => {
                        var m = Json.obj("text"->hl.content.substring(k._1,k._2),"start" -> k._1, "end" -> k._2, "terms" -> matches(k))
                        if (srp.offsetData) m = m ++ Json.obj("data"-> g(doc,k._1,srp.matchOffsetSearchType))
                        m
                      }),
                      "snippet" -> ExtendedUnifiedHighlighter.highlightToString(p, hl.content)
                    )
                  if (srp.offsetData) md = md ++ Json.obj(
                    "startData" -> g(doc, p.getStartOffset, srp.startOffsetSearchType),
                    "endData" -> g(doc, p.getEndOffset, srp.endOffsetSearchType)
                  )
                  md
                })
              ).getOrElse(Array.empty)))
            }
            df ++ Map("score" -> (if (srp.sumScaling == SumScaling.DF) Json.toJson(score) else Json.toJson(score.toInt)))
        }))
    })
  }
 
}
