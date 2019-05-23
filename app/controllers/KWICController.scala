package controllers

import java.text.BreakIterator

import com.koloboke.collect.map.hash.HashIntObjMaps
import javax.inject._
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{ScoreMode, SimpleCollector, TotalHitCountCollector}
import parameters._
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, AnyContent}
import services.IndexAccessProvider

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


@Singleton
class KWICController @Inject()(iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {

  case class KWICMatch(context: String, docId: Int, startOffset: Int, endOffset: Int, matchStartIndex: Int, matchEndIndex: Int, terms: Seq[String], sortIndices: Seq[(Int,Int)]) {
    def compare(that: KWICMatch, sd: Seq[SortDirection.Value], cs: Seq[Boolean]): Int = {
      for ((((msi,osi),csd),ccs) <- sortIndices.view.zip(that.sortIndices).zip(sd).zip(cs)) {
        var s1 = context.substring(msi._1-startOffset,msi._2-startOffset)
        var s2 = that.context.substring(osi._1-that.startOffset,osi._2-that.startOffset)
        if (!ccs) {
          s1 = s1.toLowerCase
          s2 = s2.toLowerCase
        }
        val cmp = s1.compare(s2)
        if (cmp!=0) return csd match {
          case SortDirection.ASC => cmp
          case SortDirection.DESC => -cmp
        }
      }
      0
    }
  }

  def search(index: String): Action[AnyContent] = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    implicit val qm = new QueryMetadata()
    val qp = new QueryParameters()
    val gp = new GeneralParameters()
    val srp = new KWICParameters()
    implicit val iec = gp.executionContext
    implicit val tlc = gp.tlc
    implicit val ifjp = gp.forkJoinPool
    getOrCreateResult("kwic",ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = false, qp.requiredQuery)
      val hc = new TotalHitCountCollector()
      gp.etlc.setCollector(hc)
      searcher(qlevel, QueryScoring.NONE).search(query, gp.etlc)
      qm.estimatedDocumentsToProcess = hc.getTotalHits
      qm.estimatedNumberOfResults = Math.min(hc.getTotalHits, srp.limit)
    }, () => {
      val (queryLevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, qp.requiredQuery)
      // Logger.debug(f"Final query: $query%s, level: $queryLevel%s")
      val ql = ia.indexMetadata.levelMap(queryLevel)
      val is = searcher(queryLevel,QueryScoring.TF)
      val ir = is.getIndexReader
      var total = 0
      val maxHeap = mutable.PriorityQueue.empty[KWICMatch]((x: KWICMatch, y: KWICMatch) => x.compare(y,srp.sortContextDirections,srp.sortContextCaseSensitivities))
      val processDocFields = (context: LeafReaderContext, doc: Int, getters: Map[String,Int => Option[JsValue]]) => {
        val fields = new mutable.HashMap[String, JsValue]
        for (field <- srp.fields;
             value <- getters(field)(doc)) fields += (field -> value)
        fields
      }
      val docFields = HashIntObjMaps.getDefaultFactory[collection.Map[String,JsValue]]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[collection.Map[String,JsValue]]
      val collector = new SimpleCollector() {

        override def scoreMode = ScoreMode.COMPLETE
        var context: LeafReaderContext = _

        var getters: Map[String,Int => Option[JsValue]] = _

        val da = Array(0)
        val highlighter = srp.highlighter(is, indexMetadata.indexingAnalyzers(indexMetadata.contentField),true)
        val sbi = srp.sortContextLevel(0,0)


        override def collect(ldoc: Int) {
          qm.documentsProcessed += 1
          val doc = context.docBase + ldoc
          da(0) = doc
          for (
            matchesInDoc <- highlighter.highlight(indexMetadata.contentField, query, da, Int.MaxValue - 1);
            p <- matchesInDoc.passages
          ) {
            val matches = new mutable.HashMap[(Int, Int), ArrayBuffer[String]]
            var i = 0
            while (i < p.getNumMatches) {
              matches.getOrElseUpdate((p.getMatchStarts()(i), p.getMatchEnds()(i)), new ArrayBuffer[String]()) += p.getMatchTerms()(i).utf8ToString
              i += 1
            }
            sbi.setText(matchesInDoc.content)
            var soffset = p.getStartOffset
            var eoffset = p.getEndOffset
            for ((m,terms) <- matches) {
              val sorts = (for ((dindex,pindex) <- srp.sortContextDistancesByDistance) yield {
                var startIndex = m._1
                var endIndex = m._2
                if (dindex < 0) {
                  var mdindex = dindex + 1
                  startIndex = sbi.preceding(m._1)
                  while (mdindex<0) {
                    startIndex = sbi.previous()
                    mdindex += 1
                  }
                  endIndex = sbi.next()
                } else if (dindex>0) {
                  var mdindex = dindex - 1
                  startIndex = sbi.following(m._2)
                  while (mdindex>0) {
                    startIndex = sbi.next()
                    mdindex -= 1
                  }
                  endIndex = sbi.next()
                }
                if (startIndex == BreakIterator.DONE) startIndex = 0
                if (endIndex == BreakIterator.DONE) endIndex = matchesInDoc.content.length
                if (startIndex < soffset) soffset = startIndex
                if (endIndex > eoffset) eoffset = endIndex
                (pindex,startIndex,endIndex)
              }).sortBy(_._1).map(p => (p._2,p._3))
              val k = KWICMatch(matchesInDoc.content.substring(soffset,eoffset),doc,soffset,eoffset,m._1,m._2,terms,sorts.map(p => (p._1,p._2)))
              total+=1
              if (srp.limit == -1 || total<=srp.offset + srp.limit)
                maxHeap += k
              else if (maxHeap.head.compare(k,srp.sortContextDirections,srp.sortContextCaseSensitivities)<0) {
                maxHeap.dequeue()
                maxHeap += k
              }
              i += 1
            }
          }
          if (srp.limit == -1)
            docFields.put(doc, processDocFields(context, doc, getters))
        }

        override def doSetNextReader(context: LeafReaderContext) {
          this.context = context
          if (srp.limit == -1)
            this.getters = srp.fields.map(f => f -> ql.fields(f).jsGetter(context.reader)).toMap
        }
      }
      tlc.get.setCollector(collector)
      is.search(query, gp.tlc.get)
      val values = maxHeap.dequeueAll.reverse.drop(srp.offset)
      if (srp.limit!= -1) {
        val lr = ir.leaves.get(0)
        val jsGetters = srp.fields.map(f => f -> ql.fields(f).jsGetter(lr.reader)).toMap
        values.sortBy(_.docId).foreach{p => // sort by id so that advanceExact works
          val doc = p.docId - lr.docBase
          docFields.put(p.docId, processDocFields(lr, doc, jsGetters))
        }
      }
      val g = ia.offsetDataGetter
      Left(Json.obj(
        "final_query"->query.toString,
        "total"->total,
        "matches"->values.map(kwic => {
          var md = Json.obj(
            "match"->{
              var m = Json.obj(
                "text"->kwic.context.substring(kwic.matchStartIndex-kwic.startOffset,kwic.matchEndIndex-kwic.startOffset),
                "start"->kwic.matchStartIndex,
                "end"->kwic.matchEndIndex,
                "terms"->kwic.terms
              )
              if (srp.offsetData) m = m ++ Json.obj("data"-> g(kwic.docId,kwic.matchStartIndex,srp.matchOffsetSearchType))
              m
            },
            "start"->kwic.startOffset,
            "end"->kwic.endOffset,
            "snippet"->kwic.context
          )
          if (kwic.sortIndices.nonEmpty)
            md = md ++ Json.obj(
            "sort"->kwic.sortIndices.map(p => {
              var m = Json.obj(
                "text"->kwic.context.substring(p._1-kwic.startOffset,p._2-kwic.startOffset),
                "start"->p._1,
                "end"->p._2
              )
              if (srp.offsetData) m = m ++ Json.obj("data"-> g(kwic.docId,p._1,srp.matchOffsetSearchType))
              m
            }))
          if (srp.fields.nonEmpty)
            md = md ++ Json.obj("fields"->docFields.get(kwic.docId))
          if (srp.offsetData) md = md ++ Json.obj(
            "startData" -> g(kwic.docId, kwic.startOffset, srp.startOffsetSearchType),
            "endData" -> g(kwic.docId, kwic.endOffset, srp.endOffsetSearchType)
          )
          md
        })
      ))
    })
  }
 
}
