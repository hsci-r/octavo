package controllers

import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import play.api.libs.json.JsValue
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.search.Scorer
import org.apache.lucene.index.LeafReaderContext
import play.api.libs.json.Json
import play.api.mvc.Action
import javax.inject.Inject
import org.apache.lucene.queryparser.classic.QueryParser
import play.api.mvc.Controller
import javax.inject.Named
import services.IndexAccess
import parameters.SumScaling
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import scala.collection.mutable.HashMap
import org.apache.lucene.search.TotalHitCountCollector
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.BooleanClause
import play.api.Logger
import org.apache.lucene.util.AttributeSource
import scala.collection.JavaConverters._
import org.apache.lucene.index.Term
import org.apache.lucene.search.FuzzyTermsEnum

@Singleton
class SimilarTermsController @Inject() (ia: IndexAccess) extends Controller {
  
  import ia._

  import IndexAccess._
  
  private def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] = a.foldLeft(Seq(Seq.empty[A])) {
    (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
  }
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(q: String, maxEditDistance:Int, minCommonPrefix:Int,transposeIsSingleEditg : Option[String]) = Action {
    val transposeIsSingleEdit: Boolean = transposeIsSingleEditg.exists(v => v=="" || v.toBoolean)
    val callId = s"similarTerms: query:$q, maxEditdistance:$maxEditDistance, minCommonPrefix:$minCommonPrefix, transposeIsSingleEdit:$transposeIsSingleEdit"
    Logger.info(callId)
    val qm = Json.obj("method"->"similarTerms","callId"->callId,"term"->q,"maxEditDistance"->maxEditDistance,"minCommonPrefix"->minCommonPrefix,"transposeIsSingleEdit"->transposeIsSingleEdit)
    val ts = analyzer.tokenStream(indexMetadata.contentField, q)
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
      val t = new Term(indexMetadata.contentField,qt)
      for (lrc <- reader(ia.indexMetadata.defaultLevel.id).leaves.asScala; terms = lrc.reader.terms(indexMetadata.contentField); if terms!=null; (br,docFreq) <- new FuzzyTermsEnum(terms,as,t,maxEditDistance,minCommonPrefix,transposeIsSingleEdit).asBytesRefAndDocFreqIterator)
        termMap(br.utf8ToString) += docFreq
    }
    if (parts.length==1)
      Ok(Json.obj("queryMetadata"->qm,"results"->termMaps(0).toSeq.sortBy(-_._2).map(p => Json.obj("term"->p._1,"count"->p._2))))
    else {
      val termMap = new HashMap[String, Long]()
      for (terms <- permutations(termMaps.map(_.keys.toSeq))) {
        val hc = new TotalHitCountCollector()
        val bqb = new BooleanQuery.Builder()
        var position = -1
        val pqb = new PhraseQuery.Builder()
        for ((q,o) <- terms.zip(parts.map(_._1))) {
          position += o
          pqb.add(new Term(indexMetadata.contentField,q),position)
        }
        bqb.add(pqb.build,BooleanClause.Occur.SHOULD)
        searcher(ia.indexMetadata.defaultLevel.id, SumScaling.ABSOLUTE).search(bqb.build, hc)
        if (hc.getTotalHits>0) termMap.put(terms.zip(parts.map(_._1)).map(t => "a " * (t._2 - 1) + t._1 ).mkString(" "),hc.getTotalHits)
      }
      Ok(Json.obj("queryMetadata"->qm,"results"->termMap.toSeq.sortBy(-_._2).map(p => Json.obj("term"->p._1,"count"->p._2))))
    }
  }

}