package controllers

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

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class SearchController @Inject() extends Controller {

  BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
  
  private val dir = DirectoryReader.open(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/dindex")))
  private val dis = new IndexSearcher(dir)
  private val hir = DirectoryReader.open(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/hindex")))
  private val his = new IndexSearcher(hir)
  private val pir = DirectoryReader.open(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/pindex")))
  private val pis = new IndexSearcher(pir)
  
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
  dis.setSimilarity(sim)
  private val sa = new StandardAnalyzer()
  private val analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),Map[String,Analyzer]("content"->sa,"notes"->sa,"fullTitle"->sa).asJava)
  private val dqp = new QueryParser("content",analyzer) {
    override def getRangeQuery(field: String, part1: String, part2: String, startInclusive: Boolean, endInclusive: Boolean): Query = {
      field match {
        case "pubDate" | "contentTokens" | "length" | "totalPages" =>
          val l = Try(if (startInclusive) part1.toInt else part1.toInt + 1).getOrElse(Integer.MIN_VALUE)
          val h = Try(if (endInclusive) part2.toInt else part2.toInt - 1).getOrElse(Integer.MAX_VALUE)
          IntPoint.newRangeQuery(field, l, h)
        case _ => super.getRangeQuery(field,part1,part2,startInclusive,endInclusive) 
      }
    }
  }

  private def combine[A](a: Seq[A],b: Seq[A]): Seq[Seq[A]] =
    a.zip(b).foldLeft(Seq.empty[Seq[A]]) { (x,s) => if (x.isEmpty) Seq(Seq(s._1),Seq(s._2)) else (for (a<-x) yield Seq(a:+s._1,a:+s._2)).flatten }

  private def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] =
    a.foldLeft(Seq(Seq.empty[A])) { 
      (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
    }
  
  object Level extends Enumeration {
    val DOCUMENT, SECTION, PARAGRAPH = Value
    def searcher(level: Value): IndexSearcher = {
      level match {
        case Level.PARAGRAPH => pis
        case Level.DOCUMENT => dis
        case Level.SECTION => his
      }
    }
    def reader(level: Value): IndexReader = {
      level match {
        case Level.PARAGRAPH => pir
        case Level.DOCUMENT => dir
        case Level.SECTION => hir
      }
    }
  }
  
  object Scaling extends Enumeration {
    val MIN, ABSOLUTE, FLAT = Value
  }
  
  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docs = 0l
  }
  
  private def getStats(is: IndexSearcher, q: Query): JsValue = {
    val s = new Stats
    is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = true
      
      override def setScorer(scorer: Scorer) = this.scorer = scorer

      var scorer: Scorer = null
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        s.docs += 1
        val score = scorer.score().toInt
        s.termFreqs += score
        s.totalTermFreq += score
      }
      
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Json.toJson(Map("termFreqs"->Json.toJson(s.termFreqs.sorted),"totalTermFreq"->Json.toJson(s.totalTermFreq),"docs"->Json.toJson(s.docs)))
  }
  
  def stats(query: String) = Action {
    val pq = dqp.parse(query)
    Ok(Json.prettyPrint(Json.toJson(Map("query"->Json.toJson(pq.toString),"results"->Json.toJson(Map("document"->getStats(dis,pq),"paragraph"->getStats(pis,pq),"section"->getStats(his,pq)))))))
  }
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(q: String, levelg: String, maxEditDistance:Int, minCommonPrefix:Int,transposeIsSingleEditg : Option[String]) = Action {
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
      for (lrc <- Level.reader(level).leaves.asScala; terms = lrc.reader.terms("content"); if (terms!=null)) {
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

  def dump() = Action {
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
  
  private def getAggregateContextVectorForTerms(is: IndexSearcher, terms: Seq[String], q: Query, scaling: Scaling.Value, minFreqInDoc: Int, minTermLength: Int): (Long,Long,ObjIntMap[String]) = {
   val cv = HashObjIntMaps.getDefaultFactory[String].withNullKeyAllowed(false).newUpdatableMap[String]() 
   var docFreq = 0l
   var t2 = 0l
   var totalTermFreq = 0l
   Logger.info("start search:"+q)
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        docFreq+=1
        val tv = this.context.reader.getTermVector(doc, "content")
        if (tv.size()>10000) println(tv.size())
        val tvt = tv.iterator()
        val min = if (scaling!=Scaling.MIN) 0 else terms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
        var term = tvt.next()
        while (term!=null) {
          t2+=1
          if (tvt.totalTermFreq()>=minFreqInDoc) {
            val ts = term.utf8ToString
            if (ts.length>=minTermLength) {
              val d = scaling match {
                case Scaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                case Scaling.ABSOLUTE => tvt.totalTermFreq.toInt
                case Scaling.FLAT => 1
              }
              totalTermFreq+=d
              cv.addValue(ts, d)
            }
          }
          term = tvt.next()
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(q+":"+docFreq+":"+cv.size+":"+t2+":"+totalTermFreq)
    (docFreq,totalTermFreq,cv)
  }
  
  private def getBestCollocations(is: IndexSearcher, terms: Seq[String], q: Query, scaling: Scaling.Value, mf: Int, mdf: Int, mtl: Int, l: Int): (Long,Long,Map[String,(ObjIntMap[String],Double)]) = {
    val maxHeap = PriorityQueue.empty[(String,(ObjIntMap[String],Double))](new Ordering[(String,(ObjIntMap[String],Double))] {
      override def compare(x: (String,(ObjIntMap[String],Double)), y: (String,(ObjIntMap[String],Double))) = y._2._2 compare x._2._2
    })
    var total = 0
    Logger.info("start heap building:"+q)
    val (docFreq,totalTermFreq,cv) = getAggregateContextVectorForTerms(is,terms,q,scaling,mdf,mtl)
    for ((term,freq) <- cv.asScala; if freq>=mf) {
      val score = (freq + mf).toDouble/is.getIndexReader.docFreq(new Term("content",term))
      total+=1
      if (total<=l) 
        maxHeap += ((term,(cv,score)))
      else if (maxHeap.head._2._2<score) {
        maxHeap.dequeue()
        maxHeap += ((term,(cv,score)))
      }
    }
    Logger.info("end heap building:"+q)
    (docFreq,totalTermFreq,maxHeap.toMap)
  }

  // get collocations for a term query (+ a possible limit query), for defining a topic
  def collocations() = Action { implicit request =>
    val p = new TermVectorQueryParameters
    val (docFreq, totalTermFreq, collocations) = getBestCollocations(Level.searcher(p.level),p.terms,p.combinedQuery,p.scaling,p.minTotalFreq,p.minFreqInDoc,p.minTermLength,p.limit)
    val json = Json.toJson(Map("docFreq"->Json.toJson(docFreq),"totalTermFreq"->Json.toJson(totalTermFreq),"collocations"->(if (p.mdsDimensions>0) {
      val keys = new HashSet[String] 
      collocations.values.foreach(_._1.keySet.asScala.foreach(keys += _))
      val keySeq = keys.toSeq
      val matrix = new Array[Array[Double]](collocations.size)
      collocations.values.zipWithIndex.foreach{ case ((c,_),i) => 
        matrix(i)=new Array[Double](keySeq.length)
        keySeq.zipWithIndex.foreach{ case (key,i2) => matrix(i)(i2) = c.getOrDefault(key, 0) }
      }
      /* normalize values to be between 0 and 1 */
      for (i <- 0 until keySeq.length) {
        val max = matrix.view.map(_(i)).max
        matrix.foreach(r => r(i)= r(i) / max)
      }
      // TODO: apply docFreq scaling?
      val mdsMatrix = MDSJ.stressMinimization(matrix, p.mdsDimensions)
      Json.toJson(collocations.zipWithIndex.map{ case ((term,(cv, weight)),i) => (term,Json.toJson(Map("termVector"->Json.toJson(mdsMatrix(i)),"weight"->Json.toJson(weight))))})
    } else if (p.minTermVectorFreq>=0) {
      Json.toJson(collocations.map{ case (term,(cv, weight)) => (term,weight)})
    } else Json.toJson(collocations.map{ case (term,(cv, weight)) => (term,weight)}))))
    if (p.pretty)
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  private class VectorInfo {
    var docFreq = 0l
    var totalTermFreq = 0l
    val cv: ObjIntMap[String] = HashObjIntMaps.getDefaultFactory[String].withNullKeyAllowed(false).newUpdatableMap[String]()
  }
  
  private def getAggregateContextVectorForGroupedTerms(is: IndexSearcher, terms: Seq[String], q: Query, attr: String, scaling: Scaling.Value, minFreqInDoc: Int, minTermLength: Int): collection.Map[String,VectorInfo] = {
    val cvm = new HashMap[String,VectorInfo]
    var docFreq = 0l
    var t2 = 0l
    var totalTermFreq = 0l
    Logger.info("start search:"+q)
    val attrs = Collections.singleton(attr)
    is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        docFreq+=1
        val tv = this.context.reader.getTermVector(doc, "content")
        if (tv.size()>10000) println(tv.size())
        val tvt = tv.iterator()
        val min = if (scaling!=Scaling.MIN) 0 else terms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
        val cv = cvm.getOrElseUpdate(context.reader.document(doc,attrs).get(attr), new VectorInfo) 
        var term = tvt.next()
        while (term!=null) {
          t2+=1
          cv.docFreq+=1
          if (tvt.totalTermFreq()>=minFreqInDoc) {
            val ts = term.utf8ToString
            if (ts.length>=minTermLength) {
              val d = scaling match {
                case Scaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                case Scaling.ABSOLUTE => tvt.totalTermFreq.toInt
                case Scaling.FLAT => 1
              }
              totalTermFreq+=d
              cv.totalTermFreq+=d
              cv.cv.addValue(ts, d)
            }
          }
          term = tvt.next()
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(q+":"+docFreq+":"+cvm.size+":"+t2+":"+totalTermFreq)
    cvm
  }  
  
  // calculate distance between two term vectors across a metadata variable (use to create e.g. graphs of term meaning changes)
  def termVectorDiff(terms1g: Seq[String], limitQuery1g: Option[String], terms2g: Seq[String], limitQuery2g: Option[String], attrg: Option[String], levelg: String, scalingg: String, mfg: Int, mdfg: Int, mtlg: Int, p: Option[String]) = Action { implicit request =>
    var terms1: Seq[String] = terms1g
    var limitQuery1: Option[String] = limitQuery1g
    var terms2: Seq[String] = terms2g
    var limitQuery2: Option[String] = limitQuery2g
    var attr = attrg.get
    var scaling: Scaling.Value = Scaling.withName(scalingg.toUpperCase)
    var level: Level.Value = Level.withName(levelg.toUpperCase)
    var mf: Int = mfg
    var mdf: Int = mdfg
    var mtl: Int = mtlg
    var oqb1 = new BooleanQuery.Builder().setDisableCoord(true)
    limitQuery1.foreach(lq => oqb1.add(dqp.parse(lq), Occur.MUST))
    var qb1 = new BooleanQuery.Builder().setDisableCoord(true)
    var oqb2 = new BooleanQuery.Builder().setDisableCoord(true)
    limitQuery2.foreach(lq => oqb2.add(dqp.parse(lq), Occur.MUST))
    var qb2 = new BooleanQuery.Builder().setDisableCoord(true)
    terms1.foreach(term => qb1.add(new TermQuery(new Term("content", term)), Occur.SHOULD))
    terms2.foreach(term => qb2.add(new TermQuery(new Term("content", term)), Occur.SHOULD))
    val q1 = qb1.build
    val q2 = qb2.build
    val pq1 = oqb1.add(q1, Occur.MUST).add(q2, Occur.MUST_NOT).build
    val pq2 = oqb2.add(q2, Occur.MUST).add(q1, Occur.MUST_NOT).build
    val tvm1 = getAggregateContextVectorForGroupedTerms(Level.searcher(level),terms1,pq1,attr,scaling,mdf,mtl)
    val tvm2 = getAggregateContextVectorForGroupedTerms(Level.searcher(level),terms2,pq2,attr,scaling,mdf,mtl)
    
    val json = Json.toJson((tvm1.keySet ++ tvm2.keySet).map(key => { 
      if (!tvm1.contains(key) || !tvm2.contains(key)) (key,0.0)
      else (key,Distance.cosineSimilarity(tvm1(key).cv,tvm2(key).cv))
    }).toMap)
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }


 
  private def getBestSecondOrderCollocations(is: IndexSearcher, terms: Seq[String], lq: Option[Query], scaling: Scaling.Value, mf: Int, mdf: Int, mtl: Int, l: Int): (Map[String,Double],Map[String,Double]) = {
    var oqb = new BooleanQuery.Builder().setDisableCoord(true)
    lq.foreach(lq => oqb.add(lq, Occur.MUST))
    var qb = new BooleanQuery.Builder().setDisableCoord(true)
    val (_,_,tv) = getAggregateContextVectorForTerms(is,terms,oqb.add(terms.foldLeft(qb)((q, term) => q.add(new TermQuery(new Term("content", term)), Occur.SHOULD)).build, Occur.MUST).build,scaling,mdf,mtl)
    println("tv:"+tv.size)
    val tvm = new HashMap[String,ObjIntMap[String]] // tvs for all terms in the tv of the query
    for ((term,freq) <- tv.asScala;if freq>=mf) {
      val (_,_,tv) = getAggregateContextVectorForTerms(is,Seq(term),if (lq.isDefined) new BooleanQuery.Builder().setDisableCoord(true).add(lq.get, Occur.MUST).add(new TermQuery(new Term("content",term)), Occur.MUST).build else new TermQuery(new Term("content",term)),scaling,mdf,mtl)
      tvm(term) = tv
    }
    println("tvm:"+tvm.size)
    val tvm2 = new HashMap[String,ObjIntMap[String]]
    for ((term,tv) <- tvm; (term2,freq) <- tv.asScala; if freq>=mf && !tvm2.contains(term2)) {
      val (_,_,tv) = getAggregateContextVectorForTerms(is,Seq(term),if (lq.isDefined) new BooleanQuery.Builder().setDisableCoord(true).add(lq.get, Occur.MUST).add(new TermQuery(new Term("content",term2)), Occur.MUST).build else new TermQuery(new Term("content",term2)),scaling,mdf,mtl)
      tvm2(term2)= tv
    }
    println("tvm2:"+tvm2.size)
    val maxHeap = PriorityQueue.empty[(String,(ObjIntMap[String],Double))](new Ordering[(String,(ObjIntMap[String],Double))] {
      override def compare(x: (String,ObjIntMap[String],Double), y: (String,ObjIntMap[String],Double)) = y._3 compare x._3
    })
    val ordering = new Ordering[(String,Double)] {
      override def compare(x: (String,Double), y: (String,Double)) = y._2 compare x._2
    }
    val cmaxHeap = PriorityQueue.empty[(String,Double)](ordering)
    val dmaxHeap = PriorityQueue.empty[(String,Double)](ordering)
    var total = 0
    for ((term,otv) <- tvm2;if (!otv.isEmpty)) {
      val cscore = Distance.cosineSimilarity(tv,otv)
      val dscore = Distance.diceSimilarity(tv,otv)
      total+=1
      if (total<=l) { 
        cmaxHeap += ((term,cscore))
//        jmaxHeap += ((term,jscore))
        dmaxHeap += ((term,dscore))
      } else {
        if (cmaxHeap.head._2<cscore) {
          cmaxHeap.dequeue()
          cmaxHeap += ((term,cscore))
        }
        if (dmaxHeap.head._2<dscore) {
          dmaxHeap.dequeue()
          dmaxHeap += ((term,dscore))
        }
      }
    }
    (cmaxHeap.toMap,dmaxHeap.toMap)
  } 
  
  // get second order collocations for a term - to find out what other words are talked about in a similar manner, for topic definition
  def collocations2(termsg: Seq[String], lqg: Option[String], levelg: String, scalingg: String, mfg: Int, mdfg: Int, mtlg: Int, lg: Int, p: Option[String]) = Action { implicit request =>
    var terms: Seq[String] = termsg
    var lq: Option[String] = lqg
    var scaling: Scaling.Value = Scaling.withName(scalingg.toUpperCase)
    var level: Level.Value = Level.withName(levelg.toUpperCase)
    var mf: Int = mfg
    var mdf: Int = mdfg
    var mtl: Int = mtlg
    var l: Int = lg
    val (c,d) = getBestSecondOrderCollocations(pis,terms,lq.map(dqp.parse(_)),scaling,mf,mdf,mtl,l)
    val json = Json.toJson(Map("cosine"->c,"dice"->d)) 
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  private class GeneralParameters(implicit request: Request[AnyContent]) {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val level: Level.Value = p.get("level").map(v => Level.withName(v(0).toUpperCase)).getOrElse(Level.PARAGRAPH)
    val pretty: Boolean = p.get("pretty").exists(v => v(0)=="" || v(0).toBoolean)
    val limit: Int = p.get("limit").map(_(0).toInt).getOrElse(20)
    /** minimum term frequency for term to be included in returned term vector */
    val minTermVectorFreq: Int = p.get("minTermVectorFreq").map(_(0).toInt).getOrElse(-1)
    /** amount of dimensions for dimensionally reduced term vector coordinates */
    val mdsDimensions: Int = p.get("mdsDimensions").map(_(0).toInt).getOrElse(-1)    
  }

  private class TermVectorQueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) extends GeneralParameters {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val terms: Seq[String] = p.get(prefix+"terms"+suffix).getOrElse(Seq.empty)
    val termQuery: Query = {
      val qb = new BooleanQuery.Builder
      terms.foreach(term => qb.add(new TermQuery(new Term("content", term)), Occur.SHOULD))
      qb.build
    }
    val limitQuery: Option[Query] = p.get(prefix+"limitQuery").map(v => dqp.parse(v(0)))
    val combinedQuery: Query = if (!limitQuery.isDefined) termQuery else new BooleanQuery.Builder().add(termQuery, Occur.MUST).add(limitQuery.get, Occur.MUST).build
    
    val scaling: Scaling.Value = p.get(prefix+"scaling"+suffix).map(v => Scaling.withName(v(0).toUpperCase)).getOrElse(Scaling.MIN)
    /** minimum total frequency of term to filter resulting term vector */
    val minTotalFreq: Int = p.get(prefix+"minTotalFreq"+suffix).map(_(0).toInt).getOrElse(2)
    /** minimum per document frequency of term to be added to the term vector */
    val minFreqInDoc: Int = p.get(prefix+"minFreqInDoc"+suffix).map(_(0).toInt).getOrElse(1)
    /** minimum length of term to be included in the term vector */
    val minTermLength: Int = p.get(prefix+"minTermLength"+suffix).map(_(0).toInt).getOrElse(4)
    
 }
  
  private class QueryParameters(implicit request: Request[AnyContent]) extends GeneralParameters {
    private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val query: Query = dqp.parse(p.get("query").get(0))
    val fields: Seq[String] = p.get("fields").getOrElse(Seq.empty)
    val level: Level.Value = p.get("level").map(v => Level.withName(v(0).toUpperCase)).getOrElse(Level.PARAGRAPH)
    /** minimum query match frequency for doc to be included in query results */
    val minFreq: Int = p.get("minFreq").map(_(0).toInt).getOrElse(1)
    val termVectorQuery: TermVectorQueryParameters = new TermVectorQueryParameters("compareTermVector_")
    /** return explanations and norms in search */
    val returnNorms: Boolean = p.get("returnNorms").exists(v => v(0)=="" || v(0).toBoolean)
  }
  
  def jsearch() = Action { implicit request =>
    val qp = new QueryParameters
    var total = 0
    val maxHeap = PriorityQueue.empty[(Seq[String],ObjIntMap[String],Int)](new Ordering[(Seq[String],ObjIntMap[String],Int)] {
      override def compare(x: (Seq[String],ObjIntMap[String],Int), y: (Seq[String],ObjIntMap[String],Int)) = y._3 compare x._3
    })
    val compareTermVector = if (!qp.termVectorQuery.terms.isEmpty || qp.termVectorQuery.limitQuery.isDefined) getAggregateContextVectorForTerms(Level.searcher(qp.level),qp.termVectorQuery.terms,qp.termVectorQuery.combinedQuery,qp.termVectorQuery.scaling, qp.termVectorQuery.minFreqInDoc, qp.termVectorQuery.minTermLength) 
    doSearch(Level.searcher(qp.level), qp.query, qp.fields, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term]) => {
      if (scorer.score.toInt>=qp.minFreq) {
        total+=1
        val add = if (maxHeap.head._3<scorer.score.toInt) {
          maxHeap.dequeue()
          true
        } else total<=qp.limit
        if (add) {
          val cv = if (qp.minTermVectorFreq != -1) {
            val cv = HashObjIntMaps.getDefaultFactory[String].withNullKeyAllowed(false).newUpdatableMap[String]() 
            val tvt = context.reader.getTermVector(doc, "content").iterator()
            var term = tvt.next()
            while (term!=null) {
              if (tvt.totalTermFreq >= qp.minTermVectorFreq)
                cv.addValue(term.utf8ToString, tvt.totalTermFreq.toInt)
              term = tvt.next()
            }
            cv
          } else null
          maxHeap += ((qp.fields.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString, cv, scorer.score.toInt))
        }
      }
    })
    val json = Json.toJson(Map("total"->Json.toJson(total),"fields"->Json.toJson(qp.fields :+ "Freq"),"results"->Json.toJson(Json.toJson(maxHeap.map(_._1)))))
    if (qp.pretty)
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  def search() = Action { implicit request => 
    val qp = new QueryParameters
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      var headers = qp.fields :+ "Freq"
      if (qp.minTermVectorFreq != -1) headers = headers :+ "Term Vector"      
      if (qp.returnNorms) headers = headers :+ "Explanation" :+ "TermNorms"
      w.write(headers)
      doSearch(Level.searcher(qp.level), qp.query, qp.fields, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term]) => {
        if (scorer.score().toInt>=qp.minFreq) {
          var row = qp.fields.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString
          if (qp.minTermVectorFreq != -1) {
            val cv = HashObjIntMaps.getDefaultFactory[String].withNullKeyAllowed(false).newUpdatableMap[String]() 
            val tvt = context.reader.getTermVector(doc, "content").iterator()
            var term = tvt.next()
            while (term!=null) {
              if (tvt.totalTermFreq >= qp.minTermVectorFreq)
                cv.addValue(term.utf8ToString, tvt.totalTermFreq.toInt)
              term = tvt.next()
            }
            row = row :+ cv.asScala.map(p => p._1+':'+p._2).mkString(";")
          }
          if (qp.returnNorms) row = row :+ we.explain(context, doc).toString :+ (terms.map(t => t.field+":"+t.text+":"+dir.docFreq(t)+":"+dir.totalTermFreq(t)).mkString(";"))
          w.write(row)
        }
      })
      w.close()
    }).as("text/csv")
  }
 
  private def doSearch(is: IndexSearcher, q: Query, rf: Seq[String], collector: (Int,Document,Scorer,Weight,LeafReaderContext,HashSet[Term]) => Unit) {
    val pq = q.rewrite(is.getIndexReader)
    val fs = new java.util.HashSet[String]
    for (s<-rf) fs.add(s)
    val we = pq.createWeight(is, true)
    val terms = new HashSet[Term]
    we.extractTerms(terms.asJava)

    is.search(pq, new SimpleCollector() {
    
      override def needsScores: Boolean = true
      var scorer: Scorer = null
      var context: LeafReaderContext = null

      override def setScorer(scorer: Scorer) {this.scorer=scorer}

      override def collect(doc: Int) {
        val d = is.doc(context.docBase+doc, fs)
        collector(doc,d,scorer,we,context,terms)
      }

      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    
  }
  

}
