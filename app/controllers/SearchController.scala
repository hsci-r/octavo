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

  object Order extends Ordering[(Seq[String],Int)] {
     def compare(x: (Seq[String],Int), y: (Seq[String],Int)) = y._2 compare x._2
   }
  
  def dump() = Action {
    if (dir.hasDeletions()) throw new UnsupportedOperationException("Index should not have deletions!")
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      val dfields = Seq("ESTCID","documentID","fullTitle","language","module","notes","pubDate")
      val pfields = Seq("type","contentTokens")
      w.write(dfields ++ pfields)
      var ld: Document = null
      var ldid: String = null
      val pfieldContents = pfields.map(f => new ArrayBuffer[String]())
      for (lrc<-dir.leaves();lr = lrc.reader;
      i <- 0 until lr.maxDoc) {
        val d = lr.document(i)
        val did = d.get("documentID") 
        if (ldid!=did) {
          if (ldid!=null) { 
            w.write(dfields.map(f => d.get(f)) ++ pfieldContents.map(_.mkString(";")))
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
  
  def collectTermVector(is: IndexSearcher, q: Query, ls: Int, l: Int): Map[String,Double] = {
   val cv = new HashMap[String,Double].withDefaultValue(-ls)
   is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        val tv = this.context.reader.getTermVector(doc, "content")
        val tvt = tv.iterator()
        var term = tvt.next()
        while (term!=null) {
          cv(term.utf8ToString) += tvt.docFreq
          term = tvt.next()
        }
      }
      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    cv.toSeq.map(p => (p._1,(p._2.toDouble)/is.getIndexReader.docFreq(new Term("content",p._1)))).sortWith{ case (p1,p2) => p1._2> p2._2}.take(l).toMap
  }
  
  def collocations(qg: Option[String], lsg: Int, lg: Int, p: Option[String]) = Action { implicit request =>
    var q: String = qg.getOrElse(null)
    var ls: Int = lsg
    var l: Int = lg
    request.body.asFormUrlEncoded.foreach { data => 
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("ls").map(_(0)).foreach(bls => ls = bls.toInt)
      data.get("l").map(_(0)).foreach(bl => l = bl.toInt)
    }
    val pq = dqp.parse(q) 
    val json = Json.toJson(Map("document"->collectTermVector(dis,pq,ls,l),"part"->collectTermVector(his,pq,ls,l),"paragraph"->collectTermVector(pis,pq,ls,l))) 
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }

  def jsearch(qg: Option[String], rfg: Seq[String], lg: Int, mfg: Int, pg: Option[String]) = Action { implicit request =>
    var q: String = qg.getOrElse(null)
    var rf: Seq[String] = rfg
    var l: Int = lg
    var mf: Int = mfg
    var p: Option[String] = pg
    request.body.asFormUrlEncoded.foreach { data =>
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("rf").foreach(brf => rf = rf ++ brf)
      data.get("l").map(_(0)).foreach(bl => l = bl.toInt)
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("p").map(_(0)).foreach(bp => p = Some(bp))
    }
    var total = 0
    val maxHeap = PriorityQueue.empty[(Seq[String],Int)](Order) 
    doSearch(q, rf, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term]) => {
      if (scorer.score.toInt>=mf) {
        total+=1
        if (total<=l) 
          maxHeap += ((rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString, scorer.score.toInt))
        else if (maxHeap.head._2<scorer.score.toInt) {
          maxHeap.dequeue()
          maxHeap += ((rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString, scorer.score.toInt))
        }
      }
    })
    val json = Json.toJson(Map("total"->Json.toJson(total),"fields"->Json.toJson(rf :+ "Freq" :+ "TotalLength"),"results"->Json.toJson(Json.toJson(maxHeap.map(_._1)))))
    if (p.isDefined && (p.get=="" || p.get.toBoolean))
      Ok(Json.prettyPrint(json))
    else 
      Ok(json)
  }
  
  def search(qg: Option[String], rfg: Seq[String], ng: Option[String], mfg: Int) = Action { implicit request => 
    var q: String = qg.getOrElse(null)
    var rf: Seq[String] = rfg
    var mf: Int = mfg
    var n: Boolean = ng.isDefined && (ng.get=="" || ng.get.toBoolean)
    request.body.asFormUrlEncoded.foreach { data =>
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("rf").foreach(brf => rf = rf ++ brf)
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("n").map(_(0)).foreach(bn => n = bn=="" || bn.toBoolean)
    }
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      if (n) w.write(rf :+ "Freq" :+ "Explanation" :+ "TermNorms")
      else w.write(rf :+ "Freq")
      doSearch(q, rf, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term]) => {
        if (scorer.score().toInt>=mf) {
          if (n)
            w.write(rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString :+ we.explain(context, doc).toString :+ (terms.map(t => t.field+":"+t.text+":"+dir.docFreq(t)+":"+dir.totalTermFreq(t)).mkString(";")))
          else 
            w.write(rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString)
        }
      })
      w.close()
    }).as("text/csv")
  }
 
  def doSearch(q: String, rf: Seq[String], collector: (Int,Document,Scorer,Weight,LeafReaderContext,HashSet[Term]) => Unit) {
    val pq = dqp.parse(q).rewrite(dir)
    val fs = new java.util.HashSet[String]
    for (s<-rf) fs.add(s)
    val we = pq.createWeight(dis, true)
    val terms = new HashSet[Term]
    we.extractTerms(terms)

    dis.search(pq, new SimpleCollector() {
    
      override def needsScores: Boolean = true
      var scorer: Scorer = null
      var context: LeafReaderContext = null

      override def setScorer(scorer: Scorer) {this.scorer=scorer}

      override def collect(doc: Int) {
        val d = dis.doc(context.docBase+doc, fs)
        collector(doc,d,scorer,we,context,terms)
      }

      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    
  }
  

}
