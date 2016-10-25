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
        case "pubDate" | "contentTokens" =>
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
  
  def terms(q: String, d:Int, cp:Int,transpose : Boolean) = Action {
    val ts = analyzer.tokenStream("content", q)
    val ta = ts.addAttribute(classOf[CharTermAttribute])
    val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
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
      for (lrc <- dir.leaves; terms = lrc.reader.terms("content"); if (terms!=null)) {
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
  
  def buildTermVector(is: IndexSearcher, q: Query, mf: Int, mtl: Int): Map[String,Int] = {
   val cv = new HashMap[String,Int].withDefaultValue(-mf)
   var t = 0
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        t+=1
        val tv = this.context.reader.getTermVector(doc, "content")
        val tvt = tv.iterator()
        var term = tvt.next()
        while (term!=null) {
          val ts = term.utf8ToString
          if (ts.length>=mtl)
          cv(ts) += tvt.docFreq
          term = tvt.next()
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    println(q+":"+t)
    cv.toMap
  }
  
  def collectTermVector(is: IndexSearcher, q: Query, mf: Int, mtl: Int, l: Int): Map[String,Double] = {
    val maxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    var total = 0
    for ((term,freq) <- buildTermVector(is,q,mf,mtl); if freq>=0) {
      val score = (freq + mf).toDouble/is.getIndexReader.docFreq(new Term("content",term))
      total+=1
      if (total<=l) 
        maxHeap += ((term,score))
      else if (maxHeap.head._2<score) {
        maxHeap.dequeue()
        maxHeap += ((term,score))
      }
    }
    maxHeap.toMap
  }
  
  def jaccardSimilarity(x: Map[String,Int], y: Map[String,Int]): Double = {
    var nom = 0
    var denom = 0
    for (key <- x.keySet ++ y.keySet) {
      nom+=math.min(x.getOrElse(key,0),y.getOrElse(key,0))
      denom+=math.max(x.getOrElse(key,0),y.getOrElse(key,0))
    }
    return nom.toDouble/denom
  }
  
  def diceSimilarity(x: Map[String,Int], y: Map[String,Int]): Double = {
    var nom = 0
    var denom = 0
    for (key <- x.keySet ++ y.keySet) {
      nom+=math.min(x.getOrElse(key,0),y.getOrElse(key,0))
      denom+=x.getOrElse(key,0)+y.getOrElse(key,0)
    }
    return (nom*2).toDouble/denom
  }
  
  def cosineSimilarity(t1: Map[String, Int], t2: Map[String, Int]): Double = {
     //word, t1 freq, t2 freq
     val m = scala.collection.mutable.HashMap[String, (Int, Int)]()

     val sum1 = t1.foldLeft(0d) {case (sum, (word, freq)) =>
         m += word ->(freq, 0)
         sum + freq
     }

     val sum2 = t2.foldLeft(0d) {case (sum, (word, freq)) =>
         m.get(word) match {
             case Some((freq1, _)) => m += word ->(freq1, freq)
             case None => m += word ->(0, freq)
         }
         sum + freq
     }

     val (p1, p2, p3) = m.foldLeft((0d, 0d, 0d)) {case ((s1, s2, s3), e) =>
         val fs = e._2
         val f1 = fs._1 / sum1
         val f2 = fs._2 / sum2
         (s1 + f1 * f2, s2 + f1 * f1, s3 + f2 * f2)
     }

     val cos = p1 / (Math.sqrt(p2) * Math.sqrt(p3))
     cos
 }



  def collectTermVector2(is: IndexSearcher, q: Query, mf: Int, mtl: Int, l: Int): (Map[String,Double],Map[String,Double],Map[String,Double]) = {
    val tv = buildTermVector(is,q,0,mtl)
    println("tv:"+tv.size)
    val tvm = new HashMap[String,Map[String,Int]] // tvs for all terms in the tv of the query
    for ((term,freq) <- tv;if freq>=mf)
      tvm(term) = buildTermVector(is,new TermQuery(new Term("content",term)),0,mtl)
    println("tvm:"+tvm.size)
    val tvm2 = new HashMap[String,Map[String,Int]]
    for ((term,tv) <- tvm; (term2,freq) <- tv; if freq>=mf && !tvm2.contains(term2))
      tvm2(term2)=buildTermVector(is, new BooleanQuery.Builder().add(q, Occur.MUST_NOT).add(new TermQuery(new Term("content",term2)),Occur.MUST).setDisableCoord(true).build(),0,mtl)
    println("tvm2:"+tvm2.size)
    val cmaxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    val dmaxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    val jmaxHeap = PriorityQueue.empty[(String,Double)](StringDoubleOrder)
    var total = 0
    for ((term,otv) <- tvm2;if (!otv.isEmpty)) {
      val cscore = cosineSimilarity(tv,otv)
      val jscore = jaccardSimilarity(tv,otv)
      val dscore = diceSimilarity(tv,otv)
      total+=1
      if (total<=l) { 
        cmaxHeap += ((term,cscore))
        jmaxHeap += ((term,jscore))
        dmaxHeap += ((term,dscore))
      } else {
        if (cmaxHeap.head._2<cscore) {
          cmaxHeap.dequeue()
          cmaxHeap += ((term,cscore))
        }
        if (jmaxHeap.head._2<jscore) {
          jmaxHeap.dequeue()
          jmaxHeap += ((term,jscore))
        }
        if (dmaxHeap.head._2<dscore) {
          dmaxHeap.dequeue()
          dmaxHeap += ((term,dscore))
        }
      }
    }
    (cmaxHeap.toMap,jmaxHeap.toMap,dmaxHeap.toMap)
  }

  def collocations(qg: Option[String], mfg: Int, mtlg: Int, lg: Int, p: Option[String]) = Action { implicit request =>
    var q: String = qg.getOrElse(null)
    var mf: Int = mfg
    var mtl: Int = mtlg
    var l: Int = lg
    request.body.asFormUrlEncoded.foreach { data => 
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("mtl").map(_(0)).foreach(bmtl => mtl = bmtl.toInt)
      data.get("l").map(_(0)).foreach(bl => l = bl.toInt)
    }
    val pq = dqp.parse(q) 
    val json = Json.toJson(Map("document"->collectTermVector(dis,pq,mf,mtl,l),"part"->collectTermVector(his,pq,mf,mtl,l),"paragraph"->collectTermVector(pis,pq,mf,mtl,l))) 
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  def collocations2(qg: Option[String], mfg: Int, mtlg: Int, lg: Int, p: Option[String]) = Action { implicit request =>
    var q: String = qg.getOrElse(null)
    var mf: Int = mfg
    var mtl: Int = mtlg
    var l: Int = lg
    request.body.asFormUrlEncoded.foreach { data => 
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("mtl").map(_(0)).foreach(bmtl => mtl = bmtl.toInt)
      data.get("l").map(_(0)).foreach(bl => l = bl.toInt)
    }
    val pq = dqp.parse(q) 
    val (c,j,d) = collectTermVector2(pis,pq,mf,mtl,l)
    val json = Json.toJson(Map("cosine"->c,"jaccard"->j,"dice"->d)) 
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
