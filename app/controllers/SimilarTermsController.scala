package controllers

import javax.inject.{Inject, Singleton}

import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, PositionIncrementAttribute}
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.util.{AttributeSource, BytesRef}
import parameters.{GeneralParameters, QueryMetadata, SumScaling}
import play.api.libs.json.Json
import play.api.{Configuration, Environment}
import services.{IndexAccess, IndexAccessProvider}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@Singleton
class SimilarTermsController  @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  private def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] = a.foldLeft(Seq(Seq.empty[A])) {
    (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
  }
  
  private def hasPrefix(br: BytesRef, prefix: BytesRef): Boolean = {
    if (br.length<prefix.length) return false
    val aBytes = prefix.bytes
    var aUpto = prefix.offset
    val bBytes = br.bytes
    var bUpto = br.offset
    val aStop = aUpto + prefix.length
    while(aUpto < aStop) {
      if (aBytes(aUpto) != bBytes(bUpto)) return false
      aUpto += 1
      bUpto += 1
    }
    true
  }
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(index: String, term: String, maxEditDistance:Int, minCommonPrefix:Int,transposeIsSingleEditg : Option[String]) = Action { implicit request =>
    implicit val ia = iap(index)
    import IndexAccess._
    import ia._
    val transposeIsSingleEdit: Boolean = transposeIsSingleEditg.exists(v => v == "" || v.toBoolean)
    implicit val qm = new QueryMetadata(Json.obj(
      "term" -> term,
      "maxEditDistance" -> maxEditDistance,
      "minCommonPrefix" -> minCommonPrefix,
      "transposeIsSingleEdit" -> transposeIsSingleEdit))
    val gp = new GeneralParameters()
    val ts = ia.queryAnalyzers(indexMetadata.defaultLevel.id).tokenStream(indexMetadata.contentField, term)
    val ta = ts.addAttribute(classOf[CharTermAttribute])
    val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
    ts.reset()
    val parts = new ArrayBuffer[(Int, String)]
    while (ts.incrementToken())
      parts += ((oa.getPositionIncrement, ta.toString))
    ts.end()
    ts.close()
    val termMaps = parts.map(_ => new mutable.HashMap[String, Long]().withDefaultValue(0l))
    for (((_, qt), termMap) <- parts.zip(termMaps)) {
      for (
        lrc <- reader(ia.indexMetadata.defaultLevel.id).leaves.asScala;
        terms = lrc.reader.terms(indexMetadata.contentField) if terms != null;
        (br, docFreq) <- if (qt.endsWith("*")) {
          val prefix = new BytesRef(qt.substring(0, qt.length - 1))
          val ti = terms.iterator
          ti.seekCeil(prefix)
          ti.asBytesRefAndDocFreqIterator.takeWhile(p => hasPrefix(p._1, prefix))
        } else new FuzzyTermsEnum(terms, new AttributeSource(), new Term(indexMetadata.contentField, qt), maxEditDistance, minCommonPrefix, transposeIsSingleEdit).asBytesRefAndDocFreqIterator
      ) termMap(br.utf8ToString) += docFreq
    }
    implicit val ec = gp.executionContext
    getOrCreateResult("similarTerms", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      if (parts.length == 1)
        Json.toJson(termMaps.head.toSeq.sortBy(-_._2).map(p => Json.obj("term" -> p._1, "count" -> p._2)))
      else {
        val termMap = new mutable.HashMap[String, Long]
        for (terms <- permutations(termMaps.map(_.keys.toSeq))) {
          val hc = new TotalHitCountCollector()
          val bqb = new BooleanQuery.Builder()
          var position = -1
          val pqb = new PhraseQuery.Builder()
          for ((q, o) <- terms.zip(parts.map(_._1))) {
            position += o
            pqb.add(new Term(indexMetadata.contentField, q), position)
          }
          bqb.add(pqb.build, BooleanClause.Occur.SHOULD)
          searcher(ia.indexMetadata.defaultLevel.id, SumScaling.ABSOLUTE).search(bqb.build, hc)
          if (hc.getTotalHits > 0)
            termMap.put(terms.zip(parts.map(_._1)).map(t => "? " * (t._2 - 1) + t._1).mkString(" "), hc.getTotalHits) // ? = tokens removed by the analyzer
        }
        Json.toJson(termMap.toSeq.sortBy(-_._2).map(p => Json.obj("term" -> p._1, "count" -> p._2)))
      }
    })
  }

}