package controllers

import java.text.BreakIterator

import com.koloboke.collect.map.hash.HashIntObjMaps
import javax.inject._
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{Scorer, SimpleCollector, TotalHitCountCollector}
import parameters._
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, AnyContent}
import services.IndexAccessProvider

import scala.collection.mutable


@Singleton
class KWICController @Inject()(iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {

  case class KWICMatch(context: String, docId: Int, matchStartIndex: Int, matchEndIndex: Int, sortIndices: Seq[(Int,Int,SortDirection.Value)]) extends Ordered[KWICMatch] {
    def compare(that: KWICMatch): Int = {
      for ((msi,osi) <- sortIndices.zip(that.sortIndices)) {
        val cmp = context.substring(msi._1,msi._2).compare(that.context.substring(osi._1,osi._2))
        if (cmp!=0) return msi._3 match {
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
      val hc = new TotalHitCountCollector()
      val (qlevel,query) = buildFinalQueryRunningSubQueries(exactCounts = false, qp.requiredQuery)
      searcher(qlevel, SumScaling.ABSOLUTE).search(query, hc)
      hc.getTotalHits
      qm.estimatedDocumentsToProcess = hc.getTotalHits
      qm.estimatedNumberOfResults = Math.min(hc.getTotalHits, srp.limit)
    }, () => {
      val (queryLevel,query) = buildFinalQueryRunningSubQueries(exactCounts = true, qp.requiredQuery)
      // Logger.debug(f"Final query: $query%s, level: $queryLevel%s")
      val ql = ia.indexMetadata.levelMap(queryLevel)
      val is = searcher(queryLevel,SumScaling.TTF)
      val ir = is.getIndexReader
      var total = 0
      val maxHeap = mutable.PriorityQueue.empty[KWICMatch]
      val processDocFields = (context: LeafReaderContext, doc: Int, getters: Map[String,Int => Option[JsValue]]) => {
        val fields = new mutable.HashMap[String, JsValue]
        for (field <- srp.fields;
             value <- getters(field)(doc)) fields += (field -> value)
        fields
      }
      val docFields = HashIntObjMaps.getDefaultFactory[collection.Map[String,JsValue]]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[collection.Map[String,JsValue]]
      val collector = new SimpleCollector() {

        override def needsScores: Boolean = false
        var scorer: Scorer = _
        var context: LeafReaderContext = _

        var getters: Map[String,Int => Option[JsValue]] = _

        val ia = Array(0)
        val highlighter = srp.highlighter(is, indexMetadata.indexingAnalyzers(indexMetadata.contentField))
        val sbi = srp.sortContextLevel(0,0)

        override def setScorer(scorer: Scorer) {this.scorer=scorer}

        override def collect(ldoc: Int) {
          qm.documentsProcessed += 1
          val doc = context.docBase + ldoc
          ia(0) = doc
          for (
            matchesInDoc <- highlighter.highlight(indexMetadata.contentField, query, ia, Int.MaxValue - 1);
            p <- matchesInDoc.passages
          ) {
            val matches = new mutable.HashSet[(Int, Int)]
            var i = 0
            while (i < p.getNumMatches) {
              matches += ((p.getMatchStarts()(i), p.getMatchEnds()(i)))
              i += 1
            }
            sbi.setText(matchesInDoc.content)
            var soffset = p.getStartOffset
            var eoffset = p.getEndOffset
            for (m <- matches) {
              val sorts = (for ((dindex,dir,pindex) <- srp.sortContextDistancesByDistance) yield {
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
                (pindex,startIndex,endIndex,dir)
              }).sortBy(_._1).map(p => (p._2,p._3,p._4))
              val k = KWICMatch(matchesInDoc.content.substring(soffset,eoffset),doc,m._1-soffset,m._2-soffset,sorts.map(p => (p._1-soffset,p._2-soffset,p._3)))
              total+=1
              if (srp.limit == -1 || total<=srp.offset + srp.limit)
                maxHeap += k
              else if (maxHeap.head<k) {
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
      Json.obj(
        "final_query"->query.toString,
        "total"->total,
        "matches"->values.map(kwic => {
          Json.obj(
            "match"->Json.obj(
              "text"->kwic.context.substring(kwic.matchStartIndex,kwic.matchEndIndex),
              "start"->kwic.matchStartIndex,
              "end"->kwic.matchEndIndex
            ),
            "sort"->kwic.sortIndices.map(p => Json.obj(
              "text"->kwic.context.substring(p._1,p._2),
              "start"->p._1,
              "end"->p._2
            )),
            "context"->kwic.context,
            "fields"->docFields.get(kwic.docId))
        })
      )
    })
  }
 
}
