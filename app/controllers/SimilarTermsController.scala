package controllers

import javax.inject.{Inject, Singleton}

import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, PositionIncrementAttribute}
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.util.AttributeSource
import org.apache.lucene.util.automaton.CompiledAutomaton
import parameters.{GeneralParameters, QueryMetadata, SumScaling}
import play.api.libs.json.Json
import play.api.{Configuration, Environment}
import services.IndexAccessProvider

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@Singleton
class SimilarTermsController  @Inject() (implicit iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  private def permutations[A](a: Seq[Seq[A]]): Seq[Seq[A]] = a.foldLeft(Seq(Seq.empty[A])) {
    (acc, next) => acc.flatMap { combo => next.map { num => combo :+ num } } 
  }

  class Stats(val df: Long, val ttf: Long) {}

  class StatsCollector extends SimpleCollector {
    var df: Long = 0l
    var ttf: Long = 0l
    var scorer: Scorer = _

    override def setScorer(scorer: Scorer) = this.scorer = scorer

    override def needsScores() = true

    override def collect(doc: Int) = {
      this.df += 1
      this.ttf += scorer.score().toLong
    }
  }
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(index: String, levelO: Option[String], term: String, maxEditDistance:Int, minCommonPrefix:Int,transposeIsSingleEditg : Option[String], limit:Int = 20) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    import services.IndexAccess._
    val transposeIsSingleEdit: Boolean = transposeIsSingleEditg.exists(v => v == "" || v.toBoolean)
    val level = levelO.getOrElse(indexMetadata.defaultLevel.id)
    implicit val qm = new QueryMetadata(Json.obj(
      "term" -> term,
      "level" -> level,
      "maxEditDistance" -> maxEditDistance,
      "minCommonPrefix" -> minCommonPrefix,
      "transposeIsSingleEdit" -> transposeIsSingleEdit,
      "limit"->limit))
    val gp = new GeneralParameters()
    implicit val ec = gp.executionContext
    getOrCreateResult("similarTerms", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      val ts = ia.queryAnalyzers(level).tokenStream(indexMetadata.contentField, term)
      val ta = ts.addAttribute(classOf[CharTermAttribute])
      val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
      ts.reset()
      val parts = new ArrayBuffer[(Int, String)]
      while (ts.incrementToken())
        parts += ((oa.getPositionIncrement, ta.toString))
      ts.end()
      ts.close()
      val qp = ia.termVectorQueryParsers(level).get
      val terms = reader(level).leaves.get(0).reader.terms(indexMetadata.contentField)
      var tterms = 0
      var tdf = 0l
      var tttf = 0l
      val termMap = new mutable.HashMap[String, Stats]()
      if (parts.length == 1) {
        val qt = parts(0)._2
        for ((br, docFreq, termFreq) <- (qp.parse(qt) match {
               case query: AutomatonQuery => new CompiledAutomaton(query.getAutomaton, null, true, Int.MaxValue, query.isAutomatonBinary).getTermsEnum(terms)
               case _ => new FuzzyTermsEnum(terms, new AttributeSource(), new Term(indexMetadata.contentField, qt), maxEditDistance, minCommonPrefix, transposeIsSingleEdit)
             }).asBytesRefAndDocFreqAndTotalTermFreqIterator
        ) {
          tterms += 1
          tdf += docFreq
          tttf += termFreq
          termMap.put(br.utf8ToString, new Stats(docFreq, termFreq))
        }
      } else {
        val fterms = parts.map(_ => new mutable.HashSet[String]())
        for (((_, qt), cfterms) <- parts.zip(fterms);
             br <- (qp.parse(qt) match {
               case query: AutomatonQuery => new CompiledAutomaton(query.getAutomaton, null, true, Int.MaxValue, query.isAutomatonBinary).getTermsEnum(terms)
               case _ => new FuzzyTermsEnum(terms, new AttributeSource(), new Term(indexMetadata.contentField, qt), maxEditDistance, minCommonPrefix, transposeIsSingleEdit)
             }).asBytesRefIterator) cfterms += br.utf8ToString
        for (terms <- permutations(fterms.map(_.toSeq))) {
          val hc = new StatsCollector()
          val bqb = new BooleanQuery.Builder()
          var position = -1
          val pqb = new PhraseQuery.Builder()
          for ((q, o) <- terms.zip(parts.map(_._1))) {
            position += o
            pqb.add(new Term(indexMetadata.contentField, q), position)
          }
          bqb.add(pqb.build, BooleanClause.Occur.SHOULD)
          searcher(ia.indexMetadata.defaultLevel.id, SumScaling.ABSOLUTE).search(bqb.build, hc)
          if (hc.df> 0l) {
            tterms += 1
            tdf += hc.df
            tttf += hc.ttf
            termMap.put(terms.zip(parts.map(_._1)).map(t => "? " * (t._2 - 1) + t._1).mkString(" "), new Stats(hc.df, hc.ttf)) // ? = tokens removed by the analyzer
          }
        }
      }
      var orderedTerms = termMap.toSeq.sortBy(-_._2.ttf)
      if (limit != -1) orderedTerms = orderedTerms.take(limit)
      Json.toJson("general"->Json.obj("terms"->tterms,"totalDocFreq"->tdf,"totalTermFreq"->tttf),"results"->orderedTerms.map(p => Json.obj("term" -> p._1, "docFreq" -> p._2.df, "totalTermFreq" -> p._2.ttf)))
    })
  }

}