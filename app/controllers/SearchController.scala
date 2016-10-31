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
import scala.collection.JavaConversions._
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

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class SearchController @Inject() extends Controller {

  BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
  
  val dir = DirectoryReader.open(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/dindex")))
  val dis = new IndexSearcher(dir)
  val hir = DirectoryReader.open(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/hindex")))
  val his = new IndexSearcher(hir)
  val pir = DirectoryReader.open(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/pindex")))
  val pis = new IndexSearcher(pir)
  
  val sim = new SimilarityBase() {
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
  val sa = new StandardAnalyzer()
  val analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),Map("content"->sa,"notes"->sa,"fullTitle"->sa))
  val dqp = new QueryParser("content",analyzer) {
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

  def combine[A](a: Seq[A],b: Seq[A]): Seq[Seq[A]] =
    a.zip(b).foldLeft(Seq.empty[Seq[A]]) { (x,s) => if (x.isEmpty) Seq(Seq(s._1),Seq(s._2)) else (for (a<-x) yield Seq(a:+s._1,a:+s._2)).flatten }

  def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] =
    a.foldLeft(Seq(Seq.empty[A])) { 
      (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
    }
  
  class Stats {
    var termFreqs = new ArrayBuffer[Int]
    var totalTermFreq = 0l
    var docs = 0l
  }
  
  def getStats(is: IndexSearcher, q: Query): JsValue = {
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
  
  def terms(q: String, levelg: String, d:Int, cp:Int,transpose : Boolean) = Action {
    val ts = analyzer.tokenStream("content", q)
    val ta = ts.addAttribute(classOf[CharTermAttribute])
    val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
    val level = Level.withName(levelg.toUpperCase)
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
      for (lrc <- Level.reader(level).leaves; terms = lrc.reader.terms("content"); if (terms!=null)) {
        val fte = new FuzzyTermsEnum(terms,as,t,d,cp,transpose)
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
        lrc<-dir.leaves();lr = lrc.reader;
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
  
  object StringDoubleOrder extends Ordering[(String,Double)] {
     def compare(x: (String,Double), y: (String,Double)) = y._2 compare x._2
   }
  
  var lasttime = 0
  
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
  
  def buildTermVector2(is: IndexSearcher, terms: Seq[String], q: Query, attr: String, scaling: Scaling.Value, minFreqInDoc: Int, minTermLength: Int): collection.Map[String,ObjIntMap[String]] = {
   val cvm = new HashMap[String,ObjIntMap[String]] 
   var t = 0
   var t2 = 0
   Logger.info("start search:"+q)
   val attrs = Collections.singleton(attr)
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        t+=1
        val tv = this.context.reader.getTermVector(doc, "content")
        if (tv.size()>10000) println(tv.size())
        val tvt = tv.iterator()
        val min = if (scaling!=Scaling.MIN) 0 else terms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
        val cv = cvm.getOrElseUpdate(context.reader.document(doc,attrs).get(attr), HashObjIntMaps.getDefaultFactory[String].withNullKeyAllowed(false).newUpdatableMap[String]()) 
        var term = tvt.next()
        while (term!=null) {
          t2+=1
          if (tvt.totalTermFreq()>=minFreqInDoc) {
            val ts = term.utf8ToString
            if (ts.length>=minTermLength)
              cv.addValue(ts, scaling match {
                case Scaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                case Scaling.ABSOLUTE => tvt.totalTermFreq.toInt
                case Scaling.FLAT => 1
              })
          }
          term = tvt.next()
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(q+":"+t+":"+cvm.size+":"+t2)
    //println(cvm)
    cvm
  }
  
  def buildTermVector(is: IndexSearcher, terms: Seq[String], q: Query, scaling: Scaling.Value, minFreqInDoc: Int, minTermLength: Int): ObjIntMap[String] = {
   val cv = HashObjIntMaps.getDefaultFactory[String].withNullKeyAllowed(false).newUpdatableMap[String]() 
   var t = 0
   var t2 = 0
   Logger.info("start search:"+q)
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        t+=1
        val tv = this.context.reader.getTermVector(doc, "content")
        if (tv.size()>10000) println(tv.size())
        val tvt = tv.iterator()
        val min = if (scaling!=Scaling.MIN) 0 else terms.foldLeft(0)((f,term) => if (tvt.seekExact(new BytesRef(term))) f+tvt.totalTermFreq.toInt else f)
        var term = tvt.next()
        while (term!=null) {
          t2+=1
          if (tvt.totalTermFreq()>=minFreqInDoc) {
            val ts = term.utf8ToString
            if (ts.length>=minTermLength)
              cv.addValue(ts, scaling match {
                case Scaling.MIN => math.min(min,tvt.totalTermFreq.toInt)
                case Scaling.ABSOLUTE => tvt.totalTermFreq.toInt
                case Scaling.FLAT => 1
              })
          }
          term = tvt.next()
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    Logger.info(q+":"+t+":"+cv.size+":"+t2)
    cv
  }
  
  def collectTermVector(is: IndexSearcher, terms: Seq[String], q: Query, scaling: Scaling.Value, mf: Int, mdf: Int, mtl: Int, l: Int): Map[String,Double] = {
    val maxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    var total = 0
    Logger.info("start heap building:"+q)
    for ((term,freq) <- buildTermVector(is,terms,q,scaling,mdf,mtl); if freq>=mf) {
      val score = (freq + mf).toDouble/is.getIndexReader.docFreq(new Term("content",term))
      total+=1
      if (total<=l) 
        maxHeap += ((term,score))
      else if (maxHeap.head._2<score) {
        maxHeap.dequeue()
        maxHeap += ((term,score))
      }
    }
    Logger.info("end heap building:"+q)
    maxHeap.toMap
  }
  
  def jaccardSimilarity(x: ObjIntMap[String], y: ObjIntMap[String]): Double = {
    var nom = 0
    var denom = 0
    for (key <- x.keySet ++ y.keySet) {
      nom+=math.min(x.getOrDefault(key,0),y.getOrDefault(key,0))
      denom+=math.max(x.getOrDefault(key,0),y.getOrDefault(key,0))
    }
    return nom.toDouble/denom
  }
  
  def diceSimilarity(x: ObjIntMap[String], y: ObjIntMap[String]): Double = {
    var nom = 0
    var denom = 0
    for (key <- x.keySet ++ y.keySet) {
      nom+=math.min(x.getOrDefault(key,0),y.getOrDefault(key,0))
      denom+=x.getOrDefault(key,0)+y.getOrDefault(key,0)
    }
    return (nom*2).toDouble/denom
  }
  
  def cosineSimilarity(t1: ObjIntMap[String], t2: ObjIntMap[String]): Double = {
     //word, t1 freq, t2 freq
     val m = scala.collection.mutable.HashMap[String, (Int, Int)]()

     var sum1 = 0 
     t1.forEach(new ObjIntConsumer[String] {
       override def accept(word: String, freq: Int): Unit = {
         m += word -> (freq, 0)
         sum1 += freq
       }
       
     })
     var sum2 = 0
     t2.forEach(new ObjIntConsumer[String] {
       override def accept(word: String, freq: Int): Unit = {
         m.get(word) match {
             case Some((freq1, _)) => m += word ->(freq1, freq)
             case None => m += word ->(0, freq)
         }
         sum2 += freq
       }
     })

     val (p1, p2, p3) = m.foldLeft((0d, 0d, 0d)) {case ((s1, s2, s3), e) =>
         val fs = e._2
         val f1 = fs._1.toDouble / sum1
         val f2 = fs._2.toDouble / sum2
         (s1 + f1 * f2, s2 + f1 * f1, s3 + f2 * f2)
     }

     val cos = p1 / (Math.sqrt(p2) * Math.sqrt(p3))
     cos
   }

  def collectTermVector2(is: IndexSearcher, terms: Seq[String], lq: Option[Query], scaling: Scaling.Value, mf: Int, mdf: Int, mtl: Int, l: Int): (Map[String,Double],Map[String,Double]) = {
    var oqb = new BooleanQuery.Builder().setDisableCoord(true)
    lq.foreach(lq => oqb.add(lq, Occur.MUST))
    var qb = new BooleanQuery.Builder().setDisableCoord(true)
    val tv = buildTermVector(is,terms,oqb.add(terms.foldLeft(qb)((q, term) => q.add(new TermQuery(new Term("content", term)), Occur.SHOULD)).build, Occur.MUST).build,scaling,mdf,mtl)
    println("tv:"+tv.size)
    val tvm = new HashMap[String,ObjIntMap[String]] // tvs for all terms in the tv of the query
    for ((term,freq) <- tv;if freq>=mf)
      tvm(term) = buildTermVector(is,Seq(term),if (lq.isDefined) new BooleanQuery.Builder().setDisableCoord(true).add(lq.get, Occur.MUST).add(new TermQuery(new Term("content",term)), Occur.MUST).build else new TermQuery(new Term("content",term)),scaling,mdf,mtl)
    println("tvm:"+tvm.size)
    val tvm2 = new HashMap[String,ObjIntMap[String]]
    for ((term,tv) <- tvm; (term2,freq) <- tv; if freq>=mf && !tvm2.contains(term2))
      tvm2(term2)=buildTermVector(is,Seq(term),if (lq.isDefined) new BooleanQuery.Builder().setDisableCoord(true).add(lq.get, Occur.MUST).add(new TermQuery(new Term("content",term2)), Occur.MUST).build else new TermQuery(new Term("content",term2)),scaling,mdf,mtl)
    println("tvm2:"+tvm2.size)
    val cmaxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    val dmaxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    var total = 0
    for ((term,otv) <- tvm2;if (!otv.isEmpty)) {
      val cscore = cosineSimilarity(tv,otv)
      val dscore = diceSimilarity(tv,otv)
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

  def collocations(termsg: Seq[String], lqg: Option[String], levelg: String, scalingg: String, mfg: Int, mdfg: Int, mtlg: Int, lg: Int, p: Option[String]) = Action { implicit request =>
    var terms: Seq[String] = termsg
    var lq: Option[String] = lqg
    var scaling: Scaling.Value = Scaling.withName(scalingg.toUpperCase)
    var level: Level.Value = Level.withName(levelg.toUpperCase)
    var mf: Int = mfg
    var mdf: Int = mdfg
    var mtl: Int = mtlg
    var l: Int = lg
    var oqb = new BooleanQuery.Builder().setDisableCoord(true)
    lq.foreach(lq => oqb.add(dqp.parse(lq), Occur.MUST))
    var qb = new BooleanQuery.Builder().setDisableCoord(true)
    val pq = oqb.add(terms.foldLeft(qb)((q, term) => q.add(new TermQuery(new Term("content", term)), Occur.SHOULD)).build, Occur.MUST).build
    val json = Json.toJson(collectTermVector(Level.searcher(level),terms,pq,scaling,mf,mdf,mtl,l))
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  def cdiff(terms1g: Seq[String], limitQuery1g: Option[String], terms2g: Seq[String], limitQuery2g: Option[String], attrg: Option[String], levelg: String, scalingg: String, mfg: Int, mdfg: Int, mtlg: Int, p: Option[String]) = Action { implicit request =>
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
    val tvm1 = buildTermVector2(Level.searcher(level),terms1,pq1,attr,scaling,mdf,mtl)
    val tvm2 = buildTermVector2(Level.searcher(level),terms2,pq2,attr,scaling,mdf,mtl)
    
    val json = Json.toJson((tvm1.keySet ++ tvm2.keySet).map(key => { 
      if (!tvm1.contains(key) || !tvm2.contains(key)) (key,0.0)
      else (key,cosineSimilarity(tvm1(key),tvm2(key)))
    }).toMap)
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  def collocations2(termsg: Seq[String], lqg: Option[String], levelg: String, scalingg: String, mfg: Int, mdfg: Int, mtlg: Int, lg: Int, p: Option[String]) = Action { implicit request =>
    var terms: Seq[String] = termsg
    var lq: Option[String] = lqg
    var scaling: Scaling.Value = Scaling.withName(scalingg.toUpperCase)
    var level: Level.Value = Level.withName(levelg.toUpperCase)
    var mf: Int = mfg
    var mdf: Int = mdfg
    var mtl: Int = mtlg
    var l: Int = lg
    val (c,d) = collectTermVector2(pis,terms,lq.map(dqp.parse(_)),scaling,mf,mdf,mtl,l)
    val json = Json.toJson(Map("cosine"->c,"dice"->d)) 
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  object Order extends Ordering[(Seq[String],Int)] {
     def compare(x: (Seq[String],Int), y: (Seq[String],Int)) = y._2 compare x._2
   }  

  def jsearch(qg: Option[String], fg: Seq[String], lg: Int, mfg: Int, pg: Option[String]) = Action { implicit request =>
    var q: String = qg.getOrElse(null)
    var f: Seq[String] = fg
    var l: Int = lg
    var mf: Int = mfg
    var p: Option[String] = pg
    request.body.asFormUrlEncoded.foreach { data =>
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("f").foreach(brf => f = f ++ brf)
      data.get("l").map(_(0)).foreach(bl => l = bl.toInt)
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("p").map(_(0)).foreach(bp => p = Some(bp))
    }
    val is = if (q.contains("heading:")) his else dis 
    var total = 0
    val maxHeap = PriorityQueue.empty[(Seq[String],Int)](Order)
    doSearch(is,q, f, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term]) => {
      if (scorer.score.toInt>=mf) {
        total+=1
        if (total<=l) 
          maxHeap += ((f.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString, scorer.score.toInt))
        else if (maxHeap.head._2<scorer.score.toInt) {
          maxHeap.dequeue()
          maxHeap += ((f.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString, scorer.score.toInt))
        }
      }
    })
    val json = Json.toJson(Map("total"->Json.toJson(total),"fields"->Json.toJson(f :+ "Freq"),"results"->Json.toJson(Json.toJson(maxHeap.map(_._1)))))
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  def search(qg: Option[String], fg: Seq[String], ng: Option[String], mfg: Int) = Action { implicit request => 
    var q: String = qg.getOrElse(null)
    var f: Seq[String] = fg
    var mf: Int = mfg
    var n: Boolean = ng.isDefined && (ng.get=="" || ng.get.toBoolean)
    request.body.asFormUrlEncoded.foreach { data =>
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("rf").foreach(brf => f = f ++ brf)
      data.get("f").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("n").map(_(0)).foreach(bn => n = bn=="" || bn.toBoolean)
    }
    val is = if (q.contains("heading:")) his else dis 
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      if (n) w.write(f :+ "Freq" :+ "Explanation" :+ "TermNorms")
      else w.write(f :+ "Freq")
      doSearch(is, q, f, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term]) => {
        if (scorer.score().toInt>=mf) {
          if (n)
            w.write(f.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString :+ we.explain(context, doc).toString :+ (terms.map(t => t.field+":"+t.text+":"+dir.docFreq(t)+":"+dir.totalTermFreq(t)).mkString(";")))
          else 
            w.write(f.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString)
        }
      })
      w.close()
    }).as("text/csv")
  }
 
  def doSearch(is: IndexSearcher, q: String, rf: Seq[String], collector: (Int,Document,Scorer,Weight,LeafReaderContext,HashSet[Term]) => Unit) {
    val pq = dqp.parse(q).rewrite(is.getIndexReader)
    val fs = new java.util.HashSet[String]
    for (s<-rf) fs.add(s)
    val we = pq.createWeight(is, true)
    val terms = new HashSet[Term]
    we.extractTerms(terms)

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
