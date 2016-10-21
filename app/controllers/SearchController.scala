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

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class SearchController @Inject() extends Controller {

  BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
  
  val dir = FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/index"))
  val ir = DirectoryReader.open(dir)
  val is = new IndexSearcher(ir)
  val sim = new SimilarityBase() {
    override def score(stats: BasicStats, freq: Float, docLen: Float): Float = {
      return freq
    }
    override def explain(stats: BasicStats, doc: Int, freq: Explanation, docLen: Float): Explanation = {
      return Explanation.`match`(freq.getValue,"{TF:"+freq.getValue+",DL:"+docLen.toInt+"}")
    }
    override def toString(): String = {
      ""
    }
    def decodeDocLen(len: Long): Int = {
      if (len==0) 0
      else decodeNormValue(len.toByte).toInt
    }
  }
  is.setSimilarity(sim)
  val analyzer = new StandardAnalyzer()
  val allFields = {
    val fs = new HashSet[String]
    for (r <- ir.leaves();fi <- r.reader().getFieldInfos) fs.add(fi.name)
    fs.toSeq
  }
  val readableFields = ir.document(0).getFields.map(_.name).toSeq
  val fieldsArray = allFields.toArray

  def fields = Action {
    Ok(Json.obj(
        "all"->allFields,
        "readable"->readableFields))
  }
  
  def combine[A](a: Seq[A],b: Seq[A]): Seq[Seq[A]] =
    a.zip(b).foldLeft(Seq.empty[Seq[A]]) { (x,s) => if (x.isEmpty) Seq(Seq(s._1),Seq(s._2)) else (for (a<-x) yield Seq(a:+s._1,a:+s._2)).flatten }

  def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] =
    a.foldLeft(Seq(Seq.empty[A])) { 
      (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
    }
  
  def terms(q: String, d:Int, cp:Int,transpose : Boolean, f: Seq[String]) = Action {
    val ts = analyzer.tokenStream("", q)
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
    for (((so,qt),termMap) <- parts.zip(termMaps); f <- f) {
      val as = new AttributeSource()
      val t = new Term(f,qt)
      for (lrc <- ir.leaves; terms = lrc.reader.terms(f); if (terms!=null)) {
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
      for (field <-f ) {
        var position = -1
        val pqb = new PhraseQuery.Builder()
        for ((q,o) <- terms.zip(parts.map(_._1))) {
          position += o
          pqb.add(new Term(field,q),position)
        }
        bqb.add(pqb.build,BooleanClause.Occur.SHOULD)
      }
      is.search(bqb.build, hc)
      if (hc.getTotalHits>0) termMap.put(terms.zip(parts.map(_._1)).map(t => "a " * (t._2 - 1) + t._1 ).mkString(" "),hc.getTotalHits)
    }
    Ok(Json.toJson(termMap))
  }

  object Order extends Ordering[(Seq[String],Int)] {
     def compare(x: (Seq[String],Int), y: (Seq[String],Int)) = y._2 compare x._2
   }
  
  def dump() = Action {
    if (ir.hasDeletions()) throw new UnsupportedOperationException("Index should not have deletions!")
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      val rfs = new java.util.HashSet[String]
      val allFields = {
        val fs = new HashSet[String]
        for (r <- ir.leaves();fi <- r.reader().getFieldInfos) {
         if (fi.getIndexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) rfs.add(fi.name)
         fs.add(fi.name) 
        }
        fs.toSeq
      }
      w.write(allFields.map(f => if (rfs.contains(f)) f else f + " Length"))
      for (lrc<-ir.leaves();lr = lrc.reader) {
        var i = 0
        val fieldNorms = allFields.map(t => lr.getNormValues(t))
        while (i<lr.maxDoc) {
          val d = lr.document(i,rfs)
          w.write(allFields.zip(fieldNorms).map(t => if (rfs.contains(t._1)) d.getValues(t._1).mkString(";") else (if (t._2 != null) sim.decodeDocLen(t._2.get(i)).toString else "0")))
          i+=1
        }
      }
      w.close()
    }).as("text/csv")
  }

  def jsearch(qg: Option[String], fg: Seq[String], rfg: Seq[String], lg: Int, mfg: Int) = Action { implicit request =>
    var q: String = qg.getOrElse(null)
    var rf: Seq[String] = rfg
    var f: Seq[String] = fg
    var l: Int = lg
    var mf: Int = mfg
    request.body.asFormUrlEncoded.foreach { data =>
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("f").foreach(bf => f = f ++ bf)
      data.get("rf").foreach(brf => rf = rf ++ brf)
      data.get("l").map(_(0)).foreach(bl => l = bl.toInt)
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
    }
    var total = 0
    val maxHeap = PriorityQueue.empty[(Seq[String],Int)](Order) 
    doSearch(q, f, rf, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term], norms: Seq[NumericDocValues]) => {
      if (scorer.score.toInt>=mf) {
        total+=1
        if (total<=l) 
          maxHeap += ((rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString :+ norms.foldLeft(0)((c,n) => c+sim.decodeDocLen(n.get(doc))).toString, scorer.score.toInt))
        else if (maxHeap.head._2<scorer.score.toInt) {
          maxHeap.dequeue()
          maxHeap += ((rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString :+ norms.foldLeft(0)((c,n) => c+sim.decodeDocLen(n.get(doc))).toString, scorer.score.toInt))
        }
      }
    })
    Ok(Json.toJson(Map("total"->Json.toJson(total),"fields"->Json.toJson(rf :+ "Freq" :+ "TotalLength"),"results"->Json.toJson(Json.toJson(maxHeap.map(_._1))))))
  }
  
  def search(qg: Option[String], fg: Seq[String], rfg: Seq[String], ng: Option[String], mfg: Int) = Action { implicit request => 
    var q: String = qg.getOrElse(null)
    var rf: Seq[String] = rfg
    var f: Seq[String] = fg
    var mf: Int = mfg
    var n: Boolean = ng.isDefined && (ng.get=="" || ng.get.toBoolean)
    request.body.asFormUrlEncoded.foreach { data =>
      q = data.get("q").map(_(0)).orElse(qg).get
      data.get("f").foreach(bf => f = f ++ bf)
      data.get("rf").foreach(brf => rf = rf ++ brf)
      data.get("mf").map(_(0)).foreach(bmf => mf = bmf.toInt)
      data.get("n").map(_(0)).foreach(bn => n = bn=="" || bn.toBoolean)
    }
    Ok.chunked(Enumerator.outputStream { os => 
      val w = CSVWriter(os)
      if (n) w.write(rf :+ "Freq" :+ "TotalLength" :+ "Explanation" :+ "LengthNorms" :+ "TermNorms")
      else w.write(rf :+ "Freq" :+ "TotalLength")
      doSearch(q, f, rf, (doc: Int, d: Document, scorer: Scorer, we: Weight, context: LeafReaderContext, terms: HashSet[Term], norms: Seq[NumericDocValues]) => {
        if (scorer.score().toInt>=mf) {
          if (n)
            w.write(rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString :+ norms.foldLeft(0)((c,n) => c+sim.decodeDocLen(n.get(doc))).toString :+ we.explain(context, doc).toString :+ (f.zip(norms).map(t => t._1+":"+sim.decodeDocLen(t._2.get(doc))).mkString(";"))  :+ (terms.map(t => t.field+":"+t.text+":"+ir.docFreq(t)+":"+ir.totalTermFreq(t)).mkString(";")))
          else 
            w.write(rf.map(f => d.getValues(f).mkString(";")) :+ scorer.score().toInt.toString :+ norms.foldLeft(0)((c,n) => c+sim.decodeDocLen(n.get(doc))).toString)
        }
      })
      w.close()
    }).as("text/csv")
  }
 
  def doSearch(q: String, f: Seq[String], rf: Seq[String], collector: (Int,Document,Scorer,Weight,LeafReaderContext,HashSet[Term],Seq[NumericDocValues]) => Unit) {
    val pq = new MultiFieldQueryParser(f.toArray, analyzer).parse(q).rewrite(ir)
    val fs = new java.util.HashSet[String]
    for (s<-rf) fs.add(s)
    val we = pq.createWeight(is, true)
    val terms = new HashSet[Term]
    we.extractTerms(terms)

    is.search(pq, new SimpleCollector() {
    
      override def needsScores: Boolean = true
      var scorer: Scorer = null
      var context: LeafReaderContext = null
      var norms: Seq[NumericDocValues] = null

      override def setScorer(scorer: Scorer) {this.scorer=scorer}

      override def collect(doc: Int) {
        val d = is.doc(context.docBase+doc, fs)
        collector(doc,d,scorer,we,context,terms,norms)
      }

      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
        this.norms = f.map(f => context.reader.getNormValues(f))
      }
    })
    
  }
  

}
