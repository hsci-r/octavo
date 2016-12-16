package controllers

import enumeratum._
import javax.inject._
import play.api._
import play.api.mvc._
import play.api.libs.iteratee.Enumerator
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.store.FSDirectory
import java.nio.file.FileSystems
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.analysis.standard.StandardAnalyzer

import scala.concurrent.ExecutionContext.Implicits.global
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
import scala.collection.parallel.mutable.ParHashMap
import com.koloboke.collect.set.LongSet
import com.koloboke.collect.set.hash.HashLongSets
import java.util.function.LongConsumer

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class SearchController @Inject() extends Controller {
  
  BooleanQuery.setMaxClauseCount(Int.MaxValue)

  private val sim = new SimilarityBase() {
    override def score(stats: BasicStats, freq: Float, docLen: Float): Float = {
      return freq
    }
    override def explain(stats: BasicStats, doc: Int, freq: Explanation, docLen: Float): Explanation = {
      return Explanation.`match`(freq.getValue,"")
    }
    override def toString(): String = {
      ""
    }
  }

  private val dir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/dindex")))
  private val dis = {
    val is = new IndexSearcher(dir)
    is.setSimilarity(sim)
    is
  }
  private val dpir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/dpindex")))
  private val dpis = {
    val is = new IndexSearcher(dpir)
    is.setSimilarity(sim)
    is
  }
  private val sir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/sindex")))
  private val sis = {
    val is = new IndexSearcher(sir)
    is.setSimilarity(sim)
    is
  }
  private val pir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/pindex")))
  private val pis = {
    val is = new IndexSearcher(pir)
    is.setSimilarity(sim)
    is
  }
  
  private val sa = new StandardAnalyzer()
  private val analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),Map[String,Analyzer]("content"->sa,"notes"->sa,"fullTitle"->sa).asJava)
  private val dqp = new QueryParser("content",analyzer) {
    override def getRangeQuery(field: String, part1: String, part2: String, startInclusive: Boolean, endInclusive: Boolean): Query = {
      field match {
        case "pubDate" | "contentTokens" | "length" | "totalPages" =>
          val low = Try(if (startInclusive) part1.toInt else part1.toInt + 1).getOrElse(Int.MinValue)
          val high = Try(if (endInclusive) part2.toInt else part2.toInt - 1).getOrElse(Int.MaxValue)
          IntPoint.newRangeQuery(field, low, high)
        case _ => super.getRangeQuery(field,part1,part2,startInclusive,endInclusive) 
      }
    }
  }

  private def combine[A](a: Seq[A],b: Seq[A]): Seq[Seq[A]] =
    a.zip(b).foldLeft(Seq.empty[Seq[A]]) { (x,s) => if (x.isEmpty) Seq(Seq(s._1),Seq(s._2)) else (for (a<-x) yield Seq(a:+s._1,a:+s._2)).flatten }

  private def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] = a.foldLeft(Seq(Seq.empty[A])) {
    (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
  }
  
  sealed abstract class Level(val searcher: IndexSearcher, val reader: IndexReader) extends EnumEntry
  
  object Level extends Enum[Level] {
    val values = findValues

    case object PARAGRAPH extends Level(pis,pir)
    case object SECTION extends Level(sis,sir)
    case object DOCUMENTPART extends Level(dpis,dpir)
    case object DOCUMENT extends Level(dis,dir)
  }
  
  sealed trait SumTermVectorScaling extends EnumEntry {
    def apply(term: Long, freq: Int)(implicit ir: IndexReader): Double
  }
  
  object SumTermVectorScaling extends Enum[SumTermVectorScaling] {
    case object ABSOLUTE extends SumTermVectorScaling {
      def apply(term: Long, freq: Int)(implicit ir: IndexReader) = freq.toDouble
    }
    case object DF extends SumTermVectorScaling {
      def apply(term: Long, freq: Int)(implicit ir: IndexReader) = freq.toDouble/df(term)
    }
    case object TTF extends SumTermVectorScaling {
      def apply(term: Long, freq: Int)(implicit ir: IndexReader) = freq.toDouble/ttf(term)
    }

    val values = findValues
  }
  
  sealed abstract class DistanceMetric extends EnumEntry {
    def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double
    def apply(t1: LongDoubleMap, t2: LongDoubleMap): Double = 1.0 - similarity(t1, t2)
  }
  
  object DistanceMetric extends Enum[DistanceMetric] {
    case object COSINE extends DistanceMetric {
      def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double = Distance.cosineSimilarity(t1, t2)
    }
    case object DICE extends DistanceMetric {
      def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double = Distance.diceSimilarity(t1, t2)
    } 
    case object JACCARD extends DistanceMetric {
      def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double = Distance.jaccardSimilarity(t1, t2)
    }
    
    val values = findValues
  }
  
  object LocalTermVectorScaling extends Enumeration {
    val MIN, ABSOLUTE, FLAT = Value
  }
  
  def df(term: Long)(implicit ir: IndexReader): Int = {
     val it = ir.leaves.get(0).reader.terms("content").iterator
     it.seekExact(term)
     return it.docFreq
  }

  def ttf(term: Long)(implicit ir: IndexReader): Long = {
    val it = ir.leaves.get(0).reader.terms("content").iterator
    it.seekExact(term)
    return it.totalTermFreq
  }
  
  def getTerm(term: Long)(implicit ir: IndexReader): String = {
    val it = ir.leaves.get(0).reader.terms("content").iterator
    it.seekExact(term)
    return it.term.utf8ToString
  }
  
  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docs = 0l
  }
  
  private def getStats(is: IndexSearcher, q: Query, gatherTermFreqsPerDoc: Boolean): JsValue = {
    val s = new Stats
    is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = true
      
      override def setScorer(scorer: Scorer) = this.scorer = scorer

      var scorer: Scorer = null
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        s.docs += 1
        val score = scorer.score().toInt
        if (gatherTermFreqsPerDoc) s.termFreqs += score
        s.totalTermFreq += score
      }
      
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Json.toJson(Map("termFreqs"->Json.toJson(s.termFreqs.sorted),"totalTermFreq"->Json.toJson(s.totalTermFreq),"docs"->Json.toJson(s.docs)))
  }
  
  def stats(query: String, termFreqs : Option[String]) = Action {
    val pq = dqp.parse(query)
    val gatherTermFreqsPerDoc = termFreqs.exists(v => v=="" || v.toBoolean)
    Ok(Json.prettyPrint(Json.toJson(Map("query"->Json.toJson(pq.toString),"results"->Json.toJson(Map("document"->getStats(dis,pq, gatherTermFreqsPerDoc),"documentpart"->getStats(dpis,pq, gatherTermFreqsPerDoc),"paragraph"->getStats(pis,pq, gatherTermFreqsPerDoc),"section"->getStats(sis,pq, gatherTermFreqsPerDoc)))))))
  }
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(q: String, levelg: String, maxEditDistance:Int, minCommonPrefix:Int,transposeIsSingleEditg : Option[String]) = Action {
    Logger.info(s"b:similarTerms(query:$q, level:$levelg, maxEditdistance:$maxEditDistance, minCommonPrefix:$minCommonPrefix, transposeIsSingleEdit:$transposeIsSingleEditg)")
    val ts = analyzer.tokenStream("content", q)
    val ta = ts.addAttribute(classOf[CharTermAttribute])
    val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
    val level = Level.withName(levelg.toUpperCase)
    val transposeIsSingleEdit: Boolean = transposeIsSingleEditg.exists(v => v=="" || v.toBoolean)
    ts.reset()
    val parts = new ArrayBuffer[(Int,String)]
    while (ts.incrementToken()) {
      parts += ((oa.getPositionIncrement, ta.toString))
    }
    ts.end()
    ts.close()
    val termMaps = parts.map(_ => new HashMap[String,Long]().withDefaultValue(0l)).toSeq
    for (((so,qt),termMap) <- parts.zip(termMaps)) {
      val as = new AttributeSource()
      val t = new Term("content",qt)
      for (lrc <- level.reader.leaves.asScala; terms = lrc.reader.terms("content"); if (terms!=null)) {
        val fte = new FuzzyTermsEnum(terms,as,t,maxEditDistance,minCommonPrefix,transposeIsSingleEdit)
        var br = fte.next()
        while (br!=null) {
          termMap(br.utf8ToString) += fte.docFreq
          br = fte.next()
        }
      }
    }
    if (parts.length==1)
      Ok(Json.toJson(termMaps(0)))
    else {
      val termMap = new HashMap[String, Long]()
      for (terms <- permutations(termMaps.map(_.keys.toSeq))) {
        val hc = new TotalHitCountCollector()
        val bqb = new BooleanQuery.Builder()
        var position = -1
        val pqb = new PhraseQuery.Builder()
        for ((q,o) <- terms.zip(parts.map(_._1))) {
          position += o
          pqb.add(new Term("content",q),position)
        }
        bqb.add(pqb.build,BooleanClause.Occur.SHOULD)
        dis.search(bqb.build, hc)
        if (hc.getTotalHits>0) termMap.put(terms.zip(parts.map(_._1)).map(t => "a " * (t._2 - 1) + t._1 ).mkString(" "),hc.getTotalHits)
      }
      Ok(Json.toJson(termMap))
    }
  }

  def dump() = Action {
    Logger.info("dump")
    if (dir.hasDeletions()) throw new UnsupportedOperationException("Index should not have deletions!")
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      val dfields = Seq("ESTCID","documentID","fullTitle","language","module","pubDate","length","totalPages")
      val pfields = Seq("type")
      w.write(dfields ++ pfields)
      var ld: Document = null
      var ldid: String = null
      val pfieldContents = pfields.map(f => new ArrayBuffer[String]())
      for (
        lrc<-dir.leaves().asScala;lr = lrc.reader;
        i <- 0 until lr.maxDoc) {
          val d = lr.document(i)
          val did = d.get("documentID") 
          if (ldid!=did) {
            if (ldid!=null) { 
              w.write(dfields.map(f => ld.get(f)) ++ pfieldContents.map(_.mkString(";")))
              pfieldContents.foreach(_.clear)
            }
            ldid=did
            ld=d
          }
          pfields.zip(pfieldContents).foreach{ case (p,c) => c += d.get(p) }
        }
      w.write(dfields.map(f => ld.getValues(f).mkString(";")) ++ pfieldContents.map(_.mkString(";")))
      w.close()
    }).as("text/csv")
  }
  
  private def getContextTermsForTerms(q: Query, ctvp: LocalTermVectorProcessingParameters, maxDocs: Int)(implicit is: IndexSearcher, ir: IndexReader): (Long,Long,LongSet) = {
   val cv = HashLongSets.newUpdatableSet()
   var aDocFreq = 0l
   var docFreq = 0l
   var t2 = 0l
   var totalTermFreq = 0l
   val sampleProbability = math.min(maxDocs.toDouble / getHitCount(q), 1.0)
   //println(sampleProbability+", "+", "+maxDocs.toDouble+", "+getHitCount(q)+", "+q)
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        aDocFreq+=1
        if (maxDocs == -1 || sampleProbability == 1.0 || Math.random() < sampleProbability) {
          val tv = this.context.reader.getTermVector(doc, "content")
          if (tv.size()>10000) println(tv.size())
          val tvt = tv.iterator().asInstanceOf[TVTermsEnum]
          var term = tvt.nextOrd()
          var anyMatches = false
          while (term != -1l) {
            t2+=1
            if (ctvp.matches(term, tvt.totalTermFreq)) {
              anyMatches = true
              cv.add(term)
            }
            term = tvt.nextOrd()
          }
          if (anyMatches) docFreq+=1
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(f"$q%s, processed docs: $aDocFreq%,d, contributing docs: $docFreq%,d, processed terms: $t2%,d, terms in accepted term vector: ${cv.size}%,d, total accepted term freq: $totalTermFreq%,d")
    (docFreq,totalTermFreq,cv)    
  }
  
  private def getUnscaledAggregateContextVectorForTerms(q: Query, ctvp: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], maxDocs: Int)(implicit is: IndexSearcher, ir: IndexReader): (Long,Long,LongIntMap) = {
   val cv = HashLongIntMaps.newUpdatableMap() 
   var aDocFreq = 0l
   var docFreq = 0l
   var t2 = 0l
   var totalTermFreq = 0l
   val sampleProbability = math.min(maxDocs.toDouble / getHitCount(q), 1.0)
   //println(sampleProbability+", "+", "+maxDocs.toDouble+", "+getHitCount(q)+", "+q)
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        aDocFreq+=1
        if (maxDocs == -1 || sampleProbability == 1.0 || Math.random() < sampleProbability) {
          val tv = this.context.reader.getTermVector(doc, "content")
          if (tv.size()>10000) println(tv.size())
          val tvt = tv.iterator().asInstanceOf[TVTermsEnum]
          val min = if (ctvp.localScaling!=LocalTermVectorScaling.MIN) 0 else if (minScalingTerms.isEmpty) Int.MaxValue else minScalingTerms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
          var term = tvt.nextOrd()
          var anyMatches = false
          while (term != -1l) {
            t2+=1
            if (ctvp.matches(term, tvt.totalTermFreq)) {
              anyMatches = true
              val d = ctvp.localScaling match {
                case LocalTermVectorScaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                case LocalTermVectorScaling.ABSOLUTE => tvt.totalTermFreq.toInt
                case LocalTermVectorScaling.FLAT => 1
              }
              totalTermFreq+=d
              cv.addValue(term, d)
            }
            term = tvt.nextOrd()
          }
          if (anyMatches) docFreq+=1
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(f"$q%s, processed docs: $aDocFreq%,d, contributing docs: $docFreq%,d, processed terms: $t2%,d, terms in accepted term vector: ${cv.size}%,d, total accepted term freq: $totalTermFreq%,d")
    (docFreq,totalTermFreq,cv)
  }
  
  private def scaleAndFilterTermVector(cv: LongIntMap, ctvp: AggregateTermVectorProcessingParameters)(implicit ir: IndexReader): LongDoubleMap = {
    val m = HashLongDoubleMaps.newUpdatableMap()
    cv.forEach(new LongIntConsumer {
       override def accept(k: Long, v: Int) {
         if (ctvp.matches(v)) m.put(k, ctvp.sumScaling(k, v))
       }
    })
    m
  }
  
  private def getAggregateContextVectorForTerms(q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit is: IndexSearcher, ir: IndexReader): (Long,Long,LongDoubleMap) = {
    val (docFreq, totalTermFreq, cv) = getUnscaledAggregateContextVectorForTerms(q, ctvpl, minScalingTerms, maxDocs)
    (docFreq, totalTermFreq, scaleAndFilterTermVector(cv, ctvpa))
  }
  
  private def getBestCollocations(q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int, limit: Int)(implicit is: IndexSearcher, ir: IndexReader): (Long,Long,Map[Long,Double]) = {
    val maxHeap = PriorityQueue.empty[(Long,Double)](new Ordering[(Long,Double)] {
      override def compare(x: (Long,Double), y: (Long,Double)) = y._2 compare x._2
    })
    var total = 0
    val (docFreq,totalTermFreq,cv) = getAggregateContextVectorForTerms(q, ctvpl, minScalingTerms, ctvpa, maxDocs)
    cv.forEach(new LongDoubleConsumer {
      override def accept(term: Long, score: Double) {
        total+=1
        if (limit == -1 || total<=limit) 
          maxHeap += ((term,score))
        else if (maxHeap.head._2<score) {
          maxHeap.dequeue()
          maxHeap += ((term,score))
        }
      }      
    })
    (docFreq,totalTermFreq,maxHeap.toMap)
  }

  private def mds(termVectors: Iterable[LongDoubleMap], rtp: AggregateTermVectorProcessingParameters)(implicit ir: IndexReader): Array[Array[Double]] = {
    val tvms = termVectors.toSeq
    val matrix = new Array[Array[Double]](tvms.size)
    for (i <- 0 until matrix.length)
      matrix(i) = new Array[Double](matrix.length)
    for (i <- 0 until matrix.length) {
      for (j <- i + 1 until matrix.length) {
        val dis = rtp.distance(tvms(i), tvms(j))
        matrix(i)(j) = dis
        matrix(j)(i) = dis
      }
    }
    return MDSJ.stressMinimization(matrix, rtp.mdsDimensions).transpose
  }
  
  private def toStringMap(m: LongDoubleMap)(implicit ir: IndexReader): collection.Map[String,Double] = {
    val rm = new HashMap[String,Double]
    m.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        rm.put(getTerm(term),freq)
      }
    })
    rm
  }
  
  private def toStringTraversable(m: LongSet)(implicit ir: IndexReader): Traversable[String] = {
    return new Traversable[String] {
      override def foreach[U](f: String => U): Unit = {
        m.forEach(new LongConsumer {
          override def accept(term: Long) {
            f(getTerm(term))
          }
        })
      }
    }
  }
  
  private def getMatchingFields(is: IndexSearcher, q: Query, field: String): Iterable[BytesRef] = {
    val fieldS = Collections.singleton(field)
    val ret = new ArrayBuffer[BytesRef]
    is.search(q, new SimpleCollector() {
      
      override def needsScores: Boolean = false
      
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        ret += context.reader.document(doc, fieldS).getBinaryValue(field)
      }
      
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    ret
  }
  
  private val didTerm = new Term("documentId","")
  private val dpidTerm = new Term("partId","")
  private val sidTerm = new Term("sectionId","")
  
  private def getTermsFromQuery(q: Option[Query]): Seq[String] = {
    q.map(QueryTermExtractor.getTerms(_).map(_.getTerm).toSeq).getOrElse(Seq.empty)
  }
  
  // get collocations for a term query (+ a possible limit query), for defining a topic
  def collocations() = Action { implicit request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = new GeneralParameters
    implicit val ir = gp.resultLevel.reader
    implicit val is = gp.resultLevel.searcher
    val tp = new QueryParameters
    val tppl = new LocalTermVectorProcessingParameters
    val tppa = new AggregateTermVectorProcessingParameters
    val rtp = new QueryParameters("r_")
    val rtpl = new LocalTermVectorProcessingParameters("r_")
    val rtpa = new AggregateTermVectorProcessingParameters("r_")
    val termVectors = p.get("termVector").exists(v => v(0)=="" || v(0).toBoolean)
    Logger.info(s"collocations: $gp, $tp, $tppl, $tppa, $rtpl, $rtpa")
    val maxDocs = if (gp.maxDocs == -1 || gp.limit == -1) -1 else if (rtpa.mdsDimensions>0 || rtpl.defined || rtpa.defined || termVectors) gp.maxDocs / gp.limit else gp.maxDocs
    val (docFreq, totalTermFreq, collocations) = getBestCollocations(tp.getCombinedQuery(gp.resultLevel).get,tppl,getTermsFromQuery(tp.getPrimaryQuery(gp.resultLevel)),tppa,maxDocs,gp.limit)
    val json = Json.toJson(Map("docFreq"->Json.toJson(docFreq),"totalTermFreq"->Json.toJson(totalTermFreq),"collocations"->(if (rtpa.mdsDimensions>0) {
      val mdsMatrix = mds(collocations.keys.toSeq.par.map{term => 
        val termS = getTerm(term)
        getAggregateContextVectorForTerms(new TermQuery(new Term("content",termS)), rtpl, Seq(termS), rtpa, maxDocs)._3
      }.seq,rtpa)
      Json.toJson(collocations.zipWithIndex.map{ case ((term,weight),i) => (getTerm(term),Json.toJson(Map("termVector"->Json.toJson(mdsMatrix(i)),"weight"->Json.toJson(weight))))})
    } else if (rtpl.defined || rtpa.defined || termVectors) {
      Json.toJson(collocations.par.map{ case (term, weight) => {
        val termS = getTerm(term)
        (termS,Json.toJson(Map("termVector"->Json.toJson(toStringMap(getAggregateContextVectorForTerms(new TermQuery(new Term("content",termS)), rtpl, Seq(termS), rtpa, maxDocs)._3)),"weight"->Json.toJson(weight))))
      }}.seq)
    } else Json.toJson(collocations.map(p => (getTerm(p._1),p._2))))))
    if (gp.pretty)
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  private final class UnscaledVectorInfo {
    var docFreq = 0l
    var totalTermFreq = 0l
    val cv: LongIntMap = HashLongIntMaps.getDefaultFactory.newUpdatableMap()
  }
  
  private def getUnscaledAggregateContextVectorForGroupedTerms(q: Query, ctvp: LocalTermVectorProcessingParameters, terms: Seq[String], attr: String, attrLength: Int, maxDocs: Int)(implicit is: IndexSearcher, ir: IndexReader): collection.Map[String,UnscaledVectorInfo] = {
    val cvm = new HashMap[String,UnscaledVectorInfo]
    var docFreq = 0l
    var t2 = 0l
    var totalTermFreq = 0l
    val attrs = Collections.singleton(attr)
    is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        docFreq+=1
        val tv = this.context.reader.getTermVector(doc, "content")
        if (tv.size()>10000) println(tv.size())
        val tvt = tv.iterator().asInstanceOf[TVTermsEnum]
        val min = if (ctvp.localScaling!=LocalTermVectorScaling.MIN) 0 else terms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
        val cattr = context.reader.document(doc,attrs).get(attr)
        val cv = cvm.getOrElseUpdate(if (attrLength == -1) cattr else cattr.substring(0,attrLength), new UnscaledVectorInfo) 
        var term = tvt.nextOrd()
        var anyMatches = false
        while (term != -1l) {
          t2+=1
          if (ctvp.matches(term, tvt.totalTermFreq)) {
            anyMatches = true
            val d = ctvp.localScaling match {
              case LocalTermVectorScaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
              case LocalTermVectorScaling.ABSOLUTE => tvt.totalTermFreq.toInt
              case LocalTermVectorScaling.FLAT => 1
            }
            totalTermFreq+=d
            cv.totalTermFreq+=d
            cv.cv.addValue(term, d)
          }
          term = tvt.nextOrd()
        }
        if (anyMatches) cv.docFreq+=1
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(f"$q%s, processed docs: $docFreq%,d, processed terms: $t2%,d, attr groups: ${cvm.size}%,d, terms in accepted term vector: ${cvm.foldLeft(0)((total,cvi) => total+cvi._2.cv.size)}%,d, total accepted term freq: $totalTermFreq%,d")
    cvm
  }  

  private final class VectorInfo(value: UnscaledVectorInfo, ctvpa: AggregateTermVectorProcessingParameters)(implicit ir: IndexReader) {
    var docFreq = value.docFreq
    var totalTermFreq = value.totalTermFreq
    val cv: LongDoubleMap = scaleAndFilterTermVector(value.cv,ctvpa)
  }

  private def getAggregateContextVectorForGroupedTerms(q: Query, ctvpl: LocalTermVectorProcessingParameters, minScalingTerms: Seq[String], attr: String, attrLength: Int, ctvpa: AggregateTermVectorProcessingParameters, maxDocs: Int)(implicit is: IndexSearcher, ir: IndexReader): collection.Map[String,VectorInfo] = {
    getUnscaledAggregateContextVectorForGroupedTerms(q, ctvpl, minScalingTerms, attr, attrLength, maxDocs).map{ case (key,value) => (key, new VectorInfo(value,ctvpa)) }
  }
  
  private def getHitCount(q: Query)(implicit is: IndexSearcher): Long = {
    val hc = new TotalHitCountCollector()
    is.search(q,hc)
    return hc.getTotalHits
  }
  
  // calculate distance between two term vectors across a metadata variable (use to create e.g. graphs of term meaning changes)
  def termVectorDiff() = Action { implicit request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val gp = new GeneralParameters
    implicit val ir = gp.resultLevel.reader
    implicit val is = gp.resultLevel.searcher
    val tvq1 = new QueryParameters("t1_")
    val tvq2 = new QueryParameters("t2_")
    val tvlq = new QueryParameters("l_")
    val tvlqq = tvlq.getCombinedQuery(gp.resultLevel)    
    val tvpl = new LocalTermVectorProcessingParameters
    val tvpa = new AggregateTermVectorProcessingParameters
    val attr = p.get("attr").map(_(0)).get
    val attrLength = p.get("attrLength").map(_(0).toInt).getOrElse(-1)
    val excludeOther: Boolean = p.get("excludeOther").exists(v => v(0)=="" || v(0).toBoolean)
    val meaningfulTerms: Int = p.get("meaningfulTerms").map(_(0).toInt).getOrElse(0)
    val q1 = tvq1.getCombinedQuery(gp.resultLevel).get
    val q2 = tvq2.getCombinedQuery(gp.resultLevel).get
    val bqb1 = new BooleanQuery.Builder().add(q1, Occur.MUST)
    for (q <- tvlqq) bqb1.add(q, Occur.MUST)
    if (excludeOther) bqb1.add(q2, Occur.MUST_NOT)
    val bqb2 = new BooleanQuery.Builder().add(q1, Occur.MUST)
    for (q <- tvlqq) bqb2.add(q, Occur.MUST)
    if (excludeOther) bqb2.add(q2, Occur.MUST_NOT)
    Logger.info(s"termVectorDiff: $gp, $tvq1, $tvq2, $tvlq, $tvpl, $tvpa, attr:$attr, attrLength:$attrLength, excludeOther:$excludeOther. ,meaningfulTerms:$meaningfulTerms, Going to process "+f"${getHitCount(bqb1.build)}%,d+${getHitCount(bqb2.build)}%,d documents.")
    val tvm1f = Future { getAggregateContextVectorForGroupedTerms(bqb1.build,tvpl,getTermsFromQuery(tvq1.getPrimaryQuery(gp.resultLevel)),attr,attrLength,tvpa,gp.maxDocs/2) }
    val tvm2f = Future { getAggregateContextVectorForGroupedTerms(bqb2.build,tvpl,getTermsFromQuery(tvq2.getPrimaryQuery(gp.resultLevel)),attr,attrLength,tvpa,gp.maxDocs/2) }
    val tvm1 = Await.result(tvm1f, Duration.Inf)
    val tvm2 = Await.result(tvm2f, Duration.Inf)
    val obj = (tvm1.keySet ++ tvm2.keySet).map(key => Json.toJson({
      if (!tvm1.contains(key) || !tvm2.contains(key)) Map("attr"->Json.toJson(key), "distance"->null,"df1"->Json.toJson(tvm1.get(key).map(_.docFreq).getOrElse(0l)),"df2"->Json.toJson(tvm2.get(key).map(_.docFreq).getOrElse(0l)),"tf1"->Json.toJson(tvm1.get(key).map(_.totalTermFreq).getOrElse(0l)),"tf2"->Json.toJson(tvm2.get(key).map(_.totalTermFreq).getOrElse(0l)))
      else Map("attr"->Json.toJson(key), "distance"->Json.toJson(tvpa.distance(tvm1(key).cv,tvm2(key).cv)),"df1"->Json.toJson(tvm1(key).docFreq),"df2"->Json.toJson(tvm2(key).docFreq),"tf1"->Json.toJson(tvm1(key).totalTermFreq),"tf2"->Json.toJson(tvm2(key).totalTermFreq))
    }))
    val json = Json.toJson(obj)
    if (gp.pretty)
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }

  private def getTermsWithMostSimilarCollocations(query: Query, limitQuery: Option[Query], ctvpl: LocalTermVectorProcessingParameters, terms: Seq[String], ctvpa: AggregateTermVectorProcessingParameters, excludeOriginal: Boolean, maxDocs: Int, limit: Int)(implicit is: IndexSearcher, ir: IndexReader): (Map[String,Double],Map[String,Double]) = {
    val bqb = new BooleanQuery.Builder()
    bqb.add(query, Occur.MUST)
    for (lq <- limitQuery) bqb.add(lq, Occur.MUST)
    val maxDocs2 = if (maxDocs == -1) -1 else maxDocs / 3
    val (_,_,collocations) = getAggregateContextVectorForTerms(bqb.build,ctvpl,terms,ctvpa, maxDocs2)
    println("collocations: "+collocations.size)
    val futures = new ArrayBuffer[Future[LongSet]]
    val maxDocs3 = if (maxDocs == -1) -1 else maxDocs2/collocations.size
    collocations.forEach(new LongDoubleConsumer {
      override def accept(term: Long, freq: Double) {
        val terms = getTerm(term)
        futures += Future {
          val (_,_,tv) = getContextTermsForTerms(if (limitQuery.isDefined) new BooleanQuery.Builder().add(limitQuery.get, Occur.MUST).add(new TermQuery(new Term("content",terms)), Occur.MUST).build else new TermQuery(new Term("content",terms)), ctvpl, maxDocs3)
          tv
        }
      }
    })
    val collocationCollocations = HashLongSets.newUpdatableSet() // tvs for all terms in the tv of the query
    for (set <- Await.result(Future.sequence(futures), Duration.Inf))
      set.forEach(new LongConsumer() {
        override def accept(term: Long) {
          collocationCollocations.add(term)
        }
      })
    println("collocations of collocations: "+collocationCollocations.size)
    val thirdOrderCollocations = new ParHashMap[String,LongDoubleMap]
    val maxDocs4 = if (maxDocs == -1) -1 else maxDocs2/collocationCollocations.size
    val futures2 = new ArrayBuffer[Future[Unit]]
    for (term2 <- toStringTraversable(collocationCollocations)) {
      val bqp = new BooleanQuery.Builder().add(new TermQuery(new Term("content",term2)), Occur.MUST)
      if (excludeOriginal) bqp.add(query, Occur.MUST_NOT)
      for (lq <- limitQuery) bqb.add(lq, Occur.MUST)
      futures2 += Future {
        val (_,_,tv) = getAggregateContextVectorForTerms(bqp.build,ctvpl,Seq(term2),ctvpa, maxDocs4)
        thirdOrderCollocations(term2) = tv
      }
    }
    Await.ready(Future.sequence(futures2), Duration.Inf)
    println("third order collocations:"+thirdOrderCollocations.size)
    val maxHeap = PriorityQueue.empty[(String,(ObjIntMap[String],Double))](new Ordering[(String,(ObjIntMap[String],Double))] {
      override def compare(x: (String,(ObjIntMap[String],Double)), y: (String,(ObjIntMap[String],Double))) = y._2._2 compare x._2._2
    })
    val ordering = new Ordering[(String,Double)] {
      override def compare(x: (String,Double), y: (String,Double)) = y._2 compare x._2
    }
    val cmaxHeap = PriorityQueue.empty[(String,Double)](ordering)
    val dmaxHeap = PriorityQueue.empty[(String,Double)](ordering)
    var total = 0
    for ((term,otv) <- thirdOrderCollocations;if (!otv.isEmpty)) {
      val cscore = Distance.cosineSimilarity(collocations,otv)
      val dscore = Distance.diceSimilarity(collocations,otv)
      total+=1
      if (limit == -1 || total<=limit) { 
        if (cscore!=0.0) cmaxHeap += ((term,cscore))
        if (dscore!=0.0) dmaxHeap += ((term,dscore))
      } else {
        if (cmaxHeap.head._2<cscore && cscore!=0.0) {
          cmaxHeap.dequeue()
          cmaxHeap += ((term,cscore))
        }
        if (dmaxHeap.head._2<dscore && dscore!=0.0) {
          dmaxHeap.dequeue()
          dmaxHeap += ((term,dscore))
        }
      }
    }
    (cmaxHeap.toMap,dmaxHeap.toMap)
  } 
  
  // get terms with similar collocations for a term - to find out what other words are talked about in a similar manner, for topic definition
  def similarCollocations() = Action { implicit request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val excludeOriginal: Boolean = p.get("excludeOriginal").exists(v => v(0)=="" || v(0).toBoolean)    
    val gp = new GeneralParameters
    implicit val ir = gp.resultLevel.reader
    implicit val is = gp.resultLevel.searcher
    val ctv = new QueryParameters()
    val lq = new QueryParameters("l_")
    val ctvpl = new LocalTermVectorProcessingParameters()
    val ctvpa = new AggregateTermVectorProcessingParameters()
    Logger.info(s"similarCollocations: $gp, $ctv, $ctvpl, $ctvpa, excludeOriginal:$excludeOriginal")
    val (c,d) = getTermsWithMostSimilarCollocations(ctv.getCombinedQuery(gp.resultLevel).get,lq.getCombinedQuery(gp.resultLevel), ctvpl,getTermsFromQuery(ctv.getPrimaryQuery(gp.resultLevel)),ctvpa, excludeOriginal,gp.maxDocs, gp.limit)
    val json = Json.toJson(Map("cosine"->c,"dice"->d)) 
    if (gp.pretty)
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  private class LocalTermVectorProcessingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    private val minFreqInDocOpt = p.get(prefix+"minFreqInDoc"+suffix).map(_(0).toLong)
    /** minimum per document frequency of term to be added to the term vector */
    val minFreqInDoc: Long = minFreqInDocOpt.getOrElse(1l)
    private val maxFreqInDocOpt = p.get(prefix+"maxFreqInDoc"+suffix).map(_(0).toLong)
    /** maximum per document frequency of term to be added to the term vector */
    val maxFreqInDoc: Long = maxFreqInDocOpt.getOrElse(Long.MaxValue)
    private def freqInDocMatches(freq: Long): Boolean = freq>=minFreqInDoc && freq<=maxFreqInDoc
    private val localScalingOpt = p.get(prefix+"localScaling"+suffix).map(v => LocalTermVectorScaling.withName(v(0).toUpperCase))
    /** per-doc term vector scaling: absolute, min (w.r.t query term) or flat (1) */
    val localScaling: LocalTermVectorScaling.Value = localScalingOpt.getOrElse(LocalTermVectorScaling.ABSOLUTE)
    private val minTotalTermFreqOpt = p.get(prefix+"minTotalTermFreq"+suffix).map(_(0).toLong)
    /** minimum total term frequency of term to be added to the term vector */
    val minTotalTermFreq: Long = minTotalTermFreqOpt.getOrElse(1l)
    private val maxTotalTermFreqOpt = p.get(prefix+"maxTotalTermFreq"+suffix).map(_(0).toLong)
    /** maximum total term frequency of term to be added to the term vector */
    val maxTotalTermFreq: Long = maxTotalTermFreqOpt.getOrElse(Long.MaxValue)
    private def totalTermFreqMatches(term: Long)(implicit ir: IndexReader): Boolean = {
      if (minTotalTermFreq == 1 && maxTotalTermFreq==Long.MaxValue) return true
      val ttfr = ttf(term)
      return ttfr>=minTotalTermFreq && ttfr<=maxTotalTermFreq
    }
    private val minDocFreqOpt = p.get(prefix+"minDocFreq"+suffix).map(_(0).toInt)
    /** minimum total document frequency of term to be added to the term vector */
    val minDocFreq: Int = minDocFreqOpt.getOrElse(1)
    private val maxDocFreqOpt = p.get(prefix+"maxDocFreq"+suffix).map(_(0).toInt)
    /** maximum total document frequency of term to be added to the term vector */
    val maxDocFreq: Int = maxDocFreqOpt.getOrElse(Int.MaxValue)
    private def docFreqMatches(term: Long)(implicit ir: IndexReader): Boolean = {
      if (minDocFreq == 1 && maxDocFreq==Int.MaxValue) return true
      val dfr = df(term)
      return dfr>=minDocFreq && dfr<=maxDocFreq
    }
    private val minTermLengthOpt = p.get(prefix+"minTermLength"+suffix).map(_(0).toInt)
    /** minimum length of term to be included in the term vector */
    val minTermLength: Int = minTermLengthOpt.getOrElse(1)
    private val maxTermLengthOpt = p.get(prefix+"maxTermLength"+suffix).map(_(0).toInt)
    /** maximum length of term to be included in the term vector */
    val maxTermLength: Int = maxTermLengthOpt.getOrElse(Int.MaxValue)
    private def termLengthMatches(term: Long)(implicit ir: IndexReader): Boolean = {
      if (minTermLength == 1 && maxTermLength == Int.MaxValue) return true
      val terms = getTerm(term)
      return terms.length>=minTermLength && terms.length<=maxTermLength
    }
    final def matches(term: Long, freq: Long)(implicit ir: IndexReader): Boolean =
      freqInDocMatches(freq) && docFreqMatches(term) && totalTermFreqMatches(term) && termLengthMatches(term)
    val defined: Boolean = localScalingOpt.isDefined || minFreqInDocOpt.isDefined || maxFreqInDocOpt.isDefined || minTotalTermFreqOpt.isDefined || maxTotalTermFreqOpt.isDefined || minDocFreqOpt.isDefined || maxDocFreqOpt.isDefined || minTermLengthOpt.isDefined || maxTermLengthOpt.isDefined
    override def toString() = s"${prefix}localScaling$suffix:$localScaling, ${prefix}minTotalTermFreq$suffix:$minTotalTermFreq, ${prefix}maxTotalTermFreq$suffix:$maxTotalTermFreq, ${prefix}minDocFreq$suffix:$minDocFreq, ${prefix}maxDocFreq$suffix:$maxDocFreq, ${prefix}minFreqInDoc$suffix:$minFreqInDoc, ${prefix}maxFreqInDoc$suffix:$maxFreqInDoc, ${prefix}minTermLength$suffix:$minTermLength, ${prefix}maxTermLength$suffix:$maxTermLength"
  }
  
  private class AggregateTermVectorProcessingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    private val sumScalingOpt = p.get(prefix+"sumScaling"+suffix).map(v => SumTermVectorScaling.withName(v(0).toUpperCase))
    val sumScaling: SumTermVectorScaling = sumScalingOpt.getOrElse(SumTermVectorScaling.TTF)
    private val minSumFreqOpt = p.get(prefix+"minSumFreq"+suffix).map(_(0).toInt)
    /** minimum sum frequency of term to filter resulting term vector */
    val minSumFreq: Int = minSumFreqOpt.getOrElse(1)
    private val maxSumFreqOpt = p.get(prefix+"maxSumFreq"+suffix).map(_(0).toInt)
    /** maximum sum frequency of term to filter resulting term vector */
    val maxSumFreq: Int = maxSumFreqOpt.getOrElse(Int.MaxValue)
    final def matches(sumFreq: Int): Boolean = {
      (minSumFreq == 1 && maxSumFreq == Int.MaxValue) || (minSumFreq <= sumFreq && maxSumFreq >= sumFreq)
    }
    private val mdsDimensionsOpt = p.get("mdsDimensions").map(_(0).toInt)
    /** amount of dimensions for dimensionally reduced term vector coordinates */
    val mdsDimensions: Int = mdsDimensionsOpt.getOrElse(-1)    
    private val distanceOpt = p.get("distance").map(v => DistanceMetric.withName(v(0).toUpperCase))
    /** distance metric used for term vector comparisons */
    val distance: DistanceMetric = distanceOpt.getOrElse(DistanceMetric.COSINE)
    val defined: Boolean = sumScalingOpt.isDefined || minSumFreqOpt.isDefined || maxSumFreqOpt.isDefined|| mdsDimensionsOpt.isDefined || distanceOpt.isDefined
    override def toString() = s"${prefix}sumScaling$suffix:$sumScaling, ${prefix}minSumFreq$suffix:$minSumFreq, ${prefix}maxSumFreq$suffix:$maxSumFreq, ${prefix}mdsDimensions$suffix:$mdsDimensions, ${prefix}distance$suffix:$distance"
  }
  
  private class QueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) {
    protected val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val documentQuery: Option[Query] = p.get(prefix+"documentQuery"+suffix).map(v => dqp.parse(v(0)))
    val documentPartQuery: Option[Query] = p.get(prefix+"documentPartQuery"+suffix).map(v => dqp.parse(v(0)))
    val sectionQuery: Option[Query] = p.get(prefix+"sectionQuery"+suffix).map(v => dqp.parse(v(0)))
    val paragraphQuery: Option[Query] = p.get(prefix+"paragraphQuery"+suffix).map(v => dqp.parse(v(0)))
    /** minimum query match frequency for doc to be included in query results */
    val minFreq: Int = p.get(prefix+"minFreq"+suffix).map(_(0).toInt).getOrElse(1)
    def getPrimaryQuery(targetLevel: Level): Option[Query] = targetLevel match {
      case Level.PARAGRAPH => paragraphQuery
      case Level.DOCUMENT => documentQuery
      case Level.DOCUMENTPART => documentPartQuery
      case Level.SECTION => sectionQuery
    }
    def getCombinedQuery(targetLevel: Level): Option[Query] = {
      if (!defined) return None
      val bqb = new BooleanQuery.Builder()
      targetLevel match {
        case Level.PARAGRAPH =>
          for (q <- paragraphQuery) bqb.add(q, Occur.MUST)
          for (q <- sectionQuery)
            bqb.add(new AutomatonQuery(sidTerm,Automata.makeStringUnion(getMatchingFields(sis, q, "sectionId").asJavaCollection)),Occur.MUST)
          for (q <- documentPartQuery)
            bqb.add(new AutomatonQuery(dpidTerm,Automata.makeStringUnion(getMatchingFields(dpis, q, "partId").asJavaCollection)),Occur.MUST)
          for (q <- documentQuery)
            bqb.add(new AutomatonQuery(didTerm,Automata.makeStringUnion(getMatchingFields(dis, q, "documentId").asJavaCollection)),Occur.MUST)
        case Level.SECTION => 
          for (q <- paragraphQuery)
            bqb.add(new AutomatonQuery(sidTerm,Automata.makeStringUnion(getMatchingFields(pis, q, "sectionId").asJavaCollection)),Occur.MUST)
          for (q <- sectionQuery) bqb.add(q, Occur.MUST)
          for (q <- documentPartQuery)
            bqb.add(new AutomatonQuery(dpidTerm,Automata.makeStringUnion(getMatchingFields(dpis, q, "partId").asJavaCollection)),Occur.MUST)
          for (q <- documentQuery)
            bqb.add(new AutomatonQuery(didTerm,Automata.makeStringUnion(getMatchingFields(dis, q, "documentId").asJavaCollection)),Occur.MUST)
        case Level.DOCUMENTPART => 
          for (q <- paragraphQuery)
            bqb.add(new AutomatonQuery(dpidTerm,Automata.makeStringUnion(getMatchingFields(pis, q, "partId").asJavaCollection)),Occur.MUST)
          for (q <- sectionQuery)
            bqb.add(new AutomatonQuery(dpidTerm,Automata.makeStringUnion(getMatchingFields(sis, q, "partId").asJavaCollection)),Occur.MUST)
          for (q <- documentPartQuery) bqb.add(q, Occur.MUST)          
          for (q <- documentQuery)
            bqb.add(new AutomatonQuery(didTerm,Automata.makeStringUnion(getMatchingFields(dis, q, "documentId").asJavaCollection)),Occur.MUST)
        case Level.DOCUMENT =>
          for (q <- paragraphQuery)
            bqb.add(new AutomatonQuery(didTerm,Automata.makeStringUnion(getMatchingFields(pis, q, "documentId").asJavaCollection)),Occur.MUST)
          for (q <- sectionQuery)
            bqb.add(new AutomatonQuery(didTerm,Automata.makeStringUnion(getMatchingFields(sis, q, "documentId").asJavaCollection)),Occur.MUST)
          for (q <- documentPartQuery)
            bqb.add(new AutomatonQuery(didTerm,Automata.makeStringUnion(getMatchingFields(dpis, q, "documentId").asJavaCollection)),Occur.MUST)
          for (q <- documentQuery) bqb.add(q, Occur.MUST)
      }
      Some(bqb.build)
    }
    val defined = documentQuery.isDefined || documentPartQuery.isDefined || sectionQuery.isDefined || paragraphQuery.isDefined
    override def toString() = s"${prefix}documentQuery$suffix:$documentQuery, ${prefix}documentPartQuery$suffix:$documentPartQuery, ${prefix}sectionQuery$suffix:$sectionQuery, ${prefix}paragraphQuery$suffix:$paragraphQuery, ${prefix}minFreq$suffix: $minFreq"
  }
  
  private class QueryReturnParameters(implicit request: Request[AnyContent]) {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val fields: Seq[String] = p.get("field").getOrElse(Seq.empty)
    /** return explanations and norms in search */
    val returnNorms: Boolean = p.get("returnNorms").exists(v => v(0)=="" || v(0).toBoolean)
    val returnMatches: Boolean = p.get("returnMatches").exists(v => v(0)=="" || v(0).toBoolean)
    override def toString() = s"fields:$fields, returnMatches: $returnMatches, returnNorms: $returnNorms"
  }

  private class GeneralParameters(implicit request: Request[AnyContent]) {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val resultLevel: Level = p.get("resultLevel").map(v => Level.withName(v(0).toUpperCase)).getOrElse(Level.PARAGRAPH)
    val pretty: Boolean = p.get("pretty").exists(v => v(0)=="" || v(0).toBoolean)
    val limit: Int = p.get("limit").map(_(0).toInt).getOrElse(20)
    val maxDocs: Int = p.get("maxDocs").map(_(0).toInt).getOrElse(resultLevel match {
      case Level.PARAGRAPH => 50000
      case Level.SECTION => 25000
      case Level.DOCUMENTPART => 5000
      case Level.DOCUMENT => 5000
    })
    override def toString() = s"resultLevel:$resultLevel, maxDocs:$maxDocs, pretty: $pretty, limit: $limit"
  }
  
  private def getTermVectorForDocument(doc: Int, ctvpl: LocalTermVectorProcessingParameters, ctvpa: AggregateTermVectorProcessingParameters)(implicit ir: IndexReader): LongDoubleMap = {
    val cv = HashLongIntMaps.newUpdatableMap() 
    val tvt = ir.getTermVector(doc, "content").iterator.asInstanceOf[TVTermsEnum]
    var term = tvt.nextOrd()
    while (term != -1l) {
      if (ctvpl.matches(term, tvt.totalTermFreq))
        cv.addValue(term, tvt.totalTermFreq.toInt)
      term = tvt.nextOrd()
    }
    scaleAndFilterTermVector(cv, ctvpa)
  }
  
  def search() = Action { implicit request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val qp = new QueryParameters
    val gp = new GeneralParameters
    implicit val ir = gp.resultLevel.reader
    implicit val is = gp.resultLevel.searcher
    val srp = new QueryReturnParameters
    val ctv = new QueryParameters("ctv_")
    val ctvpl = new LocalTermVectorProcessingParameters()
    val ctvpa = new AggregateTermVectorProcessingParameters()
    val termVectors = p.get("termVector").exists(v => v(0)=="" || v(0).toBoolean)
    Logger.info(s"$qp, $srp, $ctv, $ctvpl, $ctvpa, $gp, termVectors:$termVectors")
    var total = 0
    val maxHeap = PriorityQueue.empty[(Int,Int)](new Ordering[(Int,Int)] {
      override def compare(x: (Int,Int), y: (Int,Int)) = y._2 compare x._2
    })
    val compareTermVector = if (ctv.defined) getAggregateContextVectorForTerms(ctv.getCombinedQuery(gp.resultLevel).get,ctvpl, getTermsFromQuery(ctv.getPrimaryQuery(gp.resultLevel)),ctvpa, gp.maxDocs) else null
    var fields = srp.fields :+ "Freq"
    val highlighter = if (srp.returnMatches) { 
      fields = fields :+ "Matches"
      new UnifiedHighlighter(null, analyzer)
    } else null
    if (ctv.defined) fields = fields :+ "Distance"
    if (termVectors || ctvpa.mdsDimensions>0) fields = fields :+ "Term Vector"
    val docFields = HashIntObjMaps.newUpdatableMap[Seq[JsValue]]
    val docVectors = HashIntObjMaps.newUpdatableMap[LongDoubleMap]
    val pq = qp.getCombinedQuery(gp.resultLevel).get
    val (we, normTerms) = if (srp.returnNorms) {
      fields = fields :+ "Explanation" :+ "TermNorms"
      getTermsFromQuery(qp.getCombinedQuery(gp.resultLevel))
      val we = pq.createWeight(is, true)
      val terms = new HashSet[Term]
      we.extractTerms(terms.asJava)
      (we,  terms)
    } else (null, null)
    is.search(pq, new SimpleCollector() {
    
      override def needsScores: Boolean = true
      var scorer: Scorer = null
      var context: LeafReaderContext = null

      override def setScorer(scorer: Scorer) {this.scorer=scorer}

      override def collect(doc: Int) {
        if (scorer.score.toInt>=qp.minFreq) {
          total+=1
          if (gp.limit == -1) {
            val d = context.reader.document(doc)
            var fields = srp.fields.map(f => Json.toJson(d.getValues(f).mkString(";"))) :+ Json.toJson(scorer.score().toInt)
            if (srp.returnMatches)
              fields = fields :+ Json.toJson(highlighter.highlightWithoutSearcher("content", pq, d.get("content"), 100).toString)
            val cv = if (termVectors || ctvpa.mdsDimensions > 0 || ctv.defined) getTermVectorForDocument(doc, ctvpl, ctvpa) else null 
            if (ctv.defined)
              fields = fields :+ Json.toJson(ctvpa.distance(cv, compareTermVector._3))
            if (srp.returnNorms)
              fields = fields :+ Json.toJson(we.explain(context, doc).toString) :+ Json.toJson(normTerms.map(t => Json.toJson(Map("field"->t.field, "term"->t.text, "docFreq"->(""+dir.docFreq(t)), "totalTermFreq"->(""+dir.totalTermFreq(t))))))
            if (termVectors && ctvpa.mdsDimensions == 0) fields :+ Json.toJson(toStringMap(cv))
            else if (ctvpa.mdsDimensions > 0) docVectors.put(doc,cv)
            docFields.put(doc, fields)
            maxHeap += ((doc, scorer.score.toInt))
          } else if (total<=gp.limit)
            maxHeap += ((doc, scorer.score.toInt))
          else if (maxHeap.head._2<scorer.score.toInt) {
            maxHeap.dequeue()
            maxHeap += ((doc, scorer.score.toInt))
          }
        }
      }

      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    if (gp.limit!= -1) {
      maxHeap.foreach(p => {
        val d = ir.document(p._1)
        var fields = srp.fields.map(f => Json.toJson(d.getValues(f).mkString(";"))) :+ Json.toJson(p._2)
        if (srp.returnMatches)
          fields = fields :+ Json.toJson(highlighter.highlightWithoutSearcher("content", pq, d.get("content"), 100).toString)
        val cv = if (termVectors || ctvpa.mdsDimensions > 0 || ctv.defined) getTermVectorForDocument(p._1, ctvpl, ctvpa) else null 
        if (ctv.defined)
          fields = fields :+ Json.toJson(ctvpa.distance(cv, compareTermVector._3))
        if (srp.returnNorms) for (lr <- ir.leaves.asScala; if lr.docBase<p._1 && lr.docBase + lr.reader.maxDoc > p._1) 
          fields = fields :+ Json.toJson(we.explain(lr, p._1 - lr.docBase).toString) :+ Json.toJson(normTerms.map(t => Json.toJson(Map("field"->t.field, "term"->t.text, "docFreq"->(""+dir.docFreq(t)), "totalTermFreq"->(""+dir.totalTermFreq(t))))))
        if (termVectors && ctvpa.mdsDimensions == 0) fields :+ Json.toJson(toStringMap(cv))
        else if (ctvpa.mdsDimensions > 0) docVectors.put(p._1,cv)
        docFields.put(p._1, fields)
      })
    }
    val cvs = if (ctvpa.mdsDimensions > 0) mds(maxHeap.map(p => docVectors.get(p._1)), ctvpa).map(Json.toJson(_)).toSeq else null
    var map = Map("total"->Json.toJson(total),"fields"->Json.toJson(fields),"results"->Json.toJson(maxHeap.zipWithIndex.map{ case ((doc,_),i) =>
      if (cvs!=null) docFields.get(doc) :+ cvs(i)
      else docFields.get(doc)
    }))
    if (gp.pretty)
      Ok(Json.prettyPrint(Json.toJson(map)))
    else 
      Ok(Json.toJson(map))
  }
 
}
