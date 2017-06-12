package controllers

import enumeratum._
import javax.inject._
import play.api._
import play.api.mvc._
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.store.FSDirectory
import java.nio.file.FileSystems
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.analysis.standard.StandardAnalyzer

import org.apache.lucene.search.Scorer
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.Collector
import akka.stream.scaladsl.Source
import scala.concurrent.ExecutionContext
import scala.collection.mutable.Queue
import akka.stream.scaladsl.StreamConverters
import java.io.OutputStream
import org.reactivestreams.Publisher
import akka.stream.OverflowStrategy
import scala.concurrent.Promise
import akka.stream.scaladsl.SourceQueueWithComplete
import com.bizo.mighty.csv.CSVWriter
import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BooleanClause.Occur
import play.api.libs.json.Json
import org.apache.lucene.search.FuzzyTermsEnum
import org.apache.lucene.search.FuzzyQuery
import org.apache.lucene.index.Term
import org.apache.lucene.search.MultiTermQuery.RewriteMethod
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.MultiTermQuery
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.util.AttributeSource
import scala.collection.mutable.HashMap
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.search.similarities.SimilarityBase
import org.apache.lucene.search.similarities.BasicStats
import org.apache.lucene.search.Explanation
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.document.Document
import org.apache.lucene.search.Weight
import scala.collection.mutable.PriorityQueue
import org.apache.lucene.analysis.tokenattributes.TypeAttribute
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.TotalHitCountCollector
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.util.BytesRef
import java.util.Collections
import org.apache.lucene.search.Query
import org.apache.lucene.queryparser.xml.builders.RangeQueryBuilder
import org.apache.lucene.document.IntPoint
import scala.util.Try
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import com.koloboke.collect.map.hash.HashObjIntMaps
import com.koloboke.collect.map.hash.HashObjIntMap
import com.koloboke.collect.map.ObjIntMap
import java.util.function.ObjIntConsumer
import org.apache.lucene.index.IndexReader
import play.api.libs.json.JsValue
import org.apache.lucene.document.Field
import org.apache.lucene.analysis.Analyzer
import services.Distance
import mdsj.MDSJ
import scala.collection.immutable.AbstractMap
import org.apache.lucene.store.MMapDirectory
import com.koloboke.collect.map.IntIntMap
import com.koloboke.collect.map.hash.HashIntIntMaps
import org.apache.lucene.codecs.compressing.OrdTermVectorsReader.TVTermsEnum
import com.koloboke.collect.map.IntDoubleMap
import com.koloboke.collect.map.hash.HashLongIntMaps
import com.koloboke.collect.map.LongIntMap
import com.koloboke.function.LongIntConsumer
import com.koloboke.collect.map.hash.HashLongDoubleMaps
import com.koloboke.collect.map.LongDoubleMap
import com.koloboke.function.LongDoubleConsumer
import com.koloboke.collect.map.hash.HashLongObjMaps
import scala.collection.AbstractIterable
import com.koloboke.collect.map.hash.HashIntObjMap
import com.koloboke.collect.map.hash.HashIntObjMaps
import scala.collection.generic.Growable
import org.apache.lucene.search.highlight.QueryTermExtractor
import org.apache.lucene.util.automaton.Automata
import org.apache.lucene.search.AutomatonQuery
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import org.apache.lucene.search.uhighlight.UnifiedHighlighter
import com.koloboke.collect.set.LongSet
import com.koloboke.collect.set.hash.HashLongSets
import java.util.function.LongConsumer
import play.api.libs.json.JsNull
import org.apache.lucene.search.TimeLimitingCollector
import java.io.File
import java.io.PrintWriter
import akka.stream.Materializer
import java.io.StringWriter
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.PriorityQueue
import java.io.ByteArrayOutputStream
import play.api.libs.json.JsArray
import org.apache.lucene.index.SortedSetDocValues
import org.apache.lucene.index.SortedDocValues
import org.apache.lucene.index.DocValues
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import org.apache.lucene.index.LeafReader
import org.apache.lucene.index.TermsEnum
import org.apache.lucene.index.Terms
import services.IndexAccess
import services.TermVectors
import parameters.SumScaling
import parameters.LocalTermVectorScaling
import parameters.GeneralParameters
import parameters.QueryReturnParameters
import parameters.LocalTermVectorProcessingParameters
import parameters.AggregateTermVectorProcessingParameters
import parameters.QueryParameters
import org.apache.lucene.search.uhighlight.PassageFormatter
import org.apache.lucene.search.uhighlight.Passage
import org.apache.lucene.search.uhighlight.DefaultPassageFormatter
import services.ExtendedUnifiedHighlighter
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource
import services.IndexAccessProvider

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class SearchController @Inject() (iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  import TermVectors._
  
/*  private def counts[T](xs: TraversableOnce[T]): Map[T, Int] = {
    xs.foldLeft(HashMap.empty[T, Int].withDefaultValue(0))((acc, x) => { acc(x) += 1; acc}).toMap
  } */
  
  def search(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val qp = QueryParameters()
    val gp = GeneralParameters()
    val srp = QueryReturnParameters()
    val ctv = QueryParameters("ctv_")
    val ctvpl = LocalTermVectorProcessingParameters("r_")
    val ctvpa = AggregateTermVectorProcessingParameters("r_")
    val termVectors = p.get("termVectors").exists(v => v(0)=="" || v(0).toBoolean)
    implicit val iec = gp.executionContext
    val qm = Json.obj("method"->"search","termVector"->termVectors) ++ qp.toJson ++ gp.toJson ++ srp.toJson ++ ctv.toJson ++ ctvpl.toJson ++ ctvpa.toJson
    getOrCreateResult(ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      implicit val tlc = gp.tlc
      implicit val ifjp = gp.forkJoinPool
      val (queryLevel,query) = buildFinalQueryRunningSubQueries(true, qp.requiredQuery)
      Logger.debug(f"Final query: $query%s, level: $queryLevel%s")
      val is = searcher(queryLevel,srp.sumScaling)
      val ir = is.getIndexReader
      var total = 0
      val maxHeap = PriorityQueue.empty[(Int,Float)](new Ordering[(Int,Float)] {
        override def compare(x: (Int,Float), y: (Int,Float)) = y._2 compare x._2
      })
      val compareTermVector = if (ctv.query.isDefined)
        getAggregateContextVectorForQuery(is, buildFinalQueryRunningSubQueries(false, ctv.query.get)._2,ctvpl, extractContentTermsFromQuery(query),ctvpa, gp.maxDocs) else null
      val (we, normTerms) = if (srp.returnNorms)
        (query.createWeight(is, true), extractContentTermsFromQuery(query))
      else (null, null)
      val processDocFields = (context: LeafReaderContext, doc: Int, sdvs: Map[String,SortedDocValues], ndvs: Map[String,NumericDocValues]) => {
        val fields = new HashMap[String, JsValue]
        for ((field, dv) <- sdvs) fields += ((field -> {
          val value = dv.get(doc).utf8ToString
          if (indexMetadata.jsonFields.contains(field)) 
            Json.parse(value)
          else Json.toJson(value)
        }))
        for ((field, dv) <- ndvs) fields += ((field -> Json.toJson(dv.get(doc))))
        val document = if (srp.returnMatches || !srp.storedSingularFields.isEmpty || !srp.storedMultiFields.isEmpty) {
          val fields = new java.util.HashSet[String]
          for (field <- srp.storedSingularFields) fields.add(field)
          for (field <- srp.storedMultiFields) fields.add(field)
          if (srp.returnMatches) fields.add(indexMetadata.contentField)
          context.reader.document(doc, fields)
        } else null
        for (field <- srp.storedSingularFields) fields += ((field -> {
          val value = document.get(field)
          if (indexMetadata.jsonFields.contains(field)) 
            Json.parse(value)
          else Json.toJson(value)
        }))
        for (field <- srp.storedMultiFields) fields += ((field -> 
          (if (indexMetadata.jsonFields.contains(field)) Json.toJson(document.getValues(field).map(Json.parse(_))) 
          else Json.toJson(document.getValues(field)))
        ))
        for (field <- srp.termVectorFields) {
          val ft = context.reader.getTermVector(doc, field)
          if (ft != null) {
            val fte = ft.iterator()
            var br = fte.next()
            val map = new HashMap[String,Int]
            while (br!=null) {
              map.put(br.utf8ToString, fte.docFreq)
              br = fte.next()
            }
            fields += ((field -> Json.toJson(map)))
          }
        }
        val cv = if (termVectors || ctvpa.defined || ctvpl.defined || ctvpa.dimensions > 0 || ctv.query.isDefined) getTermVectorForDocument(ir, doc, ctvpl, ctvpa) else null 
        if (ctv.query.isDefined)
          fields += (("distance" -> Json.toJson(ctvpa.distance(cv, compareTermVector._2))))
        if (srp.returnNorms) {
          fields += (("explanation" -> Json.toJson(we.explain(context, doc).toString)))
        fields += (("norms" -> Json.toJson(normTerms.map(t => Json.toJson(Map("term"->t, "docFreq"->(""+ir.docFreq(new Term(indexMetadata.contentField, t))), "totalTermFreq"->(""+ir.totalTermFreq(new Term(indexMetadata.contentField,t)))))))))
        }
        if (cv != null && ctvpa.dimensions == 0) fields += (("termVector" -> Json.toJson(termOrdMapToOrderedTermSeq(ir, cv).map(p=>Json.obj("term" -> p._1, "weight" -> p._2)))))
        (fields, if (ctvpa.dimensions >0) cv else null)    
      }
      val docFields = HashIntObjMaps.getDefaultFactory[collection.Map[String,JsValue]]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[collection.Map[String,JsValue]]
      val docVectorsForMDS = if (ctvpa.dimensions>0) HashIntObjMaps.getDefaultFactory[LongDoubleMap]().withKeysDomain(0, Int.MaxValue).newUpdatableMap[LongDoubleMap] else null
      val collector = new SimpleCollector() {
      
        override def needsScores: Boolean = true
        var scorer: Scorer = null
        var context: LeafReaderContext = null
        
        var sdvs: Map[String,SortedDocValues] = null
        var ndvs: Map[String,NumericDocValues] = null
  
        override def setScorer(scorer: Scorer) {this.scorer=scorer}
  
        override def collect(ldoc: Int) {
          val doc = context.docBase + ldoc
          if (scorer.score >= qp.minScore) {
            total+=1
            if (srp.limit == -1) {
              val (cdocFields, cdocVectors) = processDocFields(context, doc, sdvs, ndvs)
              docFields.put(doc, cdocFields)
              if (cdocVectors != null) docVectorsForMDS.put(doc, cdocVectors)
              maxHeap += ((doc, scorer.score))
            } else if (total<=srp.limit)
              maxHeap += ((doc, scorer.score))
            else if (maxHeap.head._2<scorer.score) {
              maxHeap.dequeue()
              maxHeap += ((doc, scorer.score))
            }
          }
        }
  
        override def doSetNextReader(context: LeafReaderContext) = {
          this.context = context
          if (srp.limit == -1) {
            this.sdvs = srp.sortedDocValuesFields.map(f => (f -> DocValues.getSorted(context.reader, f))).toMap
            this.ndvs = srp.numericDocValuesFields.map(f => (f -> DocValues.getNumeric(context.reader, f))).toMap
            
          }
        }
      }
      tlc.get.setCollector(collector)
      is.search(query, gp.tlc.get)
      if (srp.limit!= -1) {
        val dvs = new HashMap[Int,(Map[String,SortedDocValues],Map[String,NumericDocValues])]
        for (lr <- ir.leaves.asScala) {
          val sdvs = srp.sortedDocValuesFields.map(f => (f -> DocValues.getSorted(lr.reader, f))).toMap
          val ndvs = srp.numericDocValuesFields.map(f => (f -> DocValues.getNumeric(lr.reader, f))).toMap
          dvs += ((lr.docBase, (sdvs, ndvs)))          
        }
        maxHeap.foreach(p =>
          for (lr <- ir.leaves.asScala; if lr.docBase<=p._1 && lr.docBase + lr.reader.maxDoc > p._1) {            
            val (sdvs,ndvs) = dvs(lr.docBase)
            val doc = p._1 - lr.docBase
            val (cdocFields, cdocVectors) = processDocFields(lr, doc, sdvs, ndvs)
            if (ctv.query.isDefined)
              cdocFields += (("distance" -> Json.toJson(ctvpa.distance(cdocVectors, compareTermVector._2))))
            docFields.put(p._1, cdocFields)
            if (cdocVectors != null) docVectorsForMDS.put(p._1, cdocVectors)
        })
      }
      val values = maxHeap.dequeueAll.reverse
      val cvs = if (ctvpa.dimensions > 0) {
        val nonEmptyVectorsAndTheirOriginalIndices = values.map(p => docVectorsForMDS.get(p._1)).zipWithIndex.filter(p => !p._1.isEmpty)
        val mdsValues = ctvpa.dimensionalityReduction(nonEmptyVectorsAndTheirOriginalIndices.map(p => p._1), ctvpa).map(Json.toJson(_)).toSeq
        val originalIndicesToMDSValueIndices = nonEmptyVectorsAndTheirOriginalIndices.map(_._2).zipWithIndex.toMap
        values.indices.map(i => originalIndicesToMDSValueIndices.get(i).map(vi => Json.toJson(mdsValues(vi))).getOrElse(JsNull))
      } else null
      val matchesByDocs = if (srp.returnMatches) {
        val highlighter = new ExtendedUnifiedHighlighter(is, indexMetadata.indexingAnalyzers(indexMetadata.contentField)) {
          override def getOffsetSource(field: String): OffsetSource = {
            val fieldInfo = getFieldInfo(field)
            if (fieldInfo != null) {
              if (fieldInfo.getIndexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
                if (fieldInfo.hasVectors()) return OffsetSource.POSTINGS_WITH_TERM_VECTORS else return OffsetSource.POSTINGS
              if (false && fieldInfo.hasVectors()) // unfortunately we can't also check if the TV has offsets
                return OffsetSource.TERM_VECTORS;
            }
            return OffsetSource.ANALYSIS
          }
        }
        highlighter.highlight(indexMetadata.contentField, query, values.map(_._1).toArray, Int.MaxValue - 1)
      } 
      else null
      Json.obj(
	      "total"->total,
	      "docs"->values.zipWithIndex.map{ 
	        case ((doc,score),i) =>
	          var df = docFields.get(doc)
            if (cvs!=null) df = df ++ Map("termVector"->cvs(i))
            if (srp.returnMatches) df = df ++ Map("matches" -> Json.toJson(matchesByDocs(i).filter(_.contains("<b>"))))
            df ++ Map("score" -> (if (ctvpa.sumScaling != SumScaling.TTF) Json.toJson(score) else Json.toJson(score.toInt))) 
      })
    })
  }
 
}
