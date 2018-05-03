package controllers

import javax.inject.{Inject, Singleton}
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, PositionIncrementAttribute}
import org.apache.lucene.index.{PostingsEnum, Term}
import org.apache.lucene.search._
import org.apache.lucene.util.automaton.CompiledAutomaton
import org.apache.lucene.util.{AttributeSource, BytesRef}
import parameters.{GeneralParameters, QueryMetadata, SumScaling}
import play.api.libs.json.Json
import play.api.{Configuration, Environment}
import services.IndexAccessProvider

import scala.collection.Searching.Found
import scala.collection.{Searching, mutable}
import scala.collection.mutable.ArrayBuffer

@Singleton
class SimilarTermsController  @Inject() (implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  private def permutations[A](a: Iterable[A]*): Iterator[Seq[A]] = a.foldLeft(Iterator(Seq.empty[A])) {
    (acc: Iterator[Seq[A]], next: Iterable[A]) => acc.flatMap { combo: Seq[A] => next.map { num => combo :+ num } }
  }

  class Stats(var df: Long = 0,var ttf: Long = 0) {}

  class StatsCollector(val termMap: mutable.HashMap[String, Stats], val apostings: ArrayBuffer[(Int,ArrayBuffer[(String,PostingsEnum)])]) extends SimpleCollector {
    var scorer: Scorer = _

    var tdf = 0l
    var tttf = 0l

    override def setScorer(scorer: Scorer) = this.scorer = scorer

    override def needsScores() = true

    val docMatches = new mutable.HashMap[String,Stats]()

    def check(sterm: String, position: Int, rPositions: Seq[(Int,ArrayBuffer[(String,ArrayBuffer[Int])])]): Unit = {
      val tail = rPositions.tail
      val (pi,termPositions) = rPositions.head
      for ((term,positions)<-termPositions) {
        Searching.search(positions).search(position + pi) match {
          case Found(_) =>
            val cterm = sterm + ("? " * (pi + -1)) + " " + term
            if (tail.nonEmpty) check(cterm, position + pi, tail) else {
              val stats = termMap.getOrElseUpdate(cterm, new Stats())
              stats.ttf += 1
              tttf += 1
              docMatches.put(cterm,stats)
            }
          case _ =>
        }
      }
    }

    override def collect(doc: Int) = {
      docMatches.clear()
      val aFilteredPositions = new ArrayBuffer[(Int,ArrayBuffer[(String,ArrayBuffer[Int])])]
      for ((pi,ppostings) <- apostings) {
        val filteredPositions = new ArrayBuffer[(String, ArrayBuffer[Int])]
        for ((term,postings) <- ppostings) if (postings.advance(doc) == doc) {
          val positions = new ArrayBuffer[Int]
          var freq = postings.freq
          while (freq>0) {
            positions += postings.nextPosition()
            freq -= 1
          }
          filteredPositions += ((term,positions))
        }
        aFilteredPositions += ((pi,filteredPositions))
      }
      val (_,termPositions) = aFilteredPositions.head
      for ((term,positions)<-termPositions;position<-positions)
        check(term,position,aFilteredPositions.tail)
      for (stats <- docMatches.valuesIterator) {
        stats.df += 1
        tdf += 1
      }
    }
  }

  val ExtendedFuzzyQuery = "(.*)~([12]):([0-9]*):?(.*)".r
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(index: String, levelO: Option[String], query: String, limit:Int = 20) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    import services.IndexAccess._
    val level = levelO.getOrElse(indexMetadata.defaultLevel.id)
    implicit val qm = new QueryMetadata(Json.obj(
      "query" -> query,
      "level" -> level,
      "limit"->limit))
    val gp = new GeneralParameters()
    implicit val ec = gp.executionContext
    getOrCreateResult("similarTerms", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      // FIXME
    }, () => {
      val ts = ia.queryAnalyzers(level).tokenStream(indexMetadata.contentField, query)
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
      var tdf = 0l
      var tttf = 0l
      val termMap = new mutable.HashMap[String, Stats]()
      if (parts.length == 1) {
        val qt = parts(0)._2
        for ((br, docFreq, termFreq) <- qt match {
          case ExtendedFuzzyQuery(rqt,maxEdits,prefixLength,transpositions) => new FuzzyTermsEnum(terms, new AttributeSource(), new Term(indexMetadata.contentField, rqt), maxEdits.toInt, if (prefixLength=="") 0 else prefixLength.toInt, transpositions != "" && transpositions.toBoolean ).asBytesRefAndDocFreqAndTotalTermFreqIterator
          case _ => qp.parse(qt) match {
           case query: AutomatonQuery => new CompiledAutomaton(query.getAutomaton, null, true, Int.MaxValue, query.isAutomatonBinary).getTermsEnum(terms).asBytesRefAndDocFreqAndTotalTermFreqIterator
           case query: FuzzyQuery =>
             new FuzzyTermsEnum(terms, new AttributeSource(), query.getTerm, query.getMaxEdits, query.getPrefixLength,query.getTranspositions).asBytesRefAndDocFreqAndTotalTermFreqIterator
             new FuzzyTermsEnum(terms, new AttributeSource(), query.getTerm, query.getMaxEdits, query.getPrefixLength,query.getTranspositions).asBytesRefAndDocFreqAndTotalTermFreqIterator
        }}
        ) {
          tdf += docFreq
          tttf += termFreq
          termMap.put(br.utf8ToString, new Stats(docFreq, termFreq))
        }
      } else {
        val mpqb = new MultiPhraseQuery.Builder()
        var position = -1
        val postings = new ArrayBuffer[(Int,ArrayBuffer[(String,PostingsEnum)])]()
        for ((increment,token) <- parts) {
          position += increment
          val ppostings = new ArrayBuffer[(String,PostingsEnum)]()
          token match {
            case ExtendedFuzzyQuery(rqt,maxEdits,prefixLength,transpositions) =>
              val fterms = new FuzzyTermsEnum(terms, new AttributeSource(), new Term(indexMetadata.contentField, rqt), maxEdits.toInt, if (prefixLength=="") 0 else prefixLength.toInt, transpositions != "" && transpositions.toBoolean )
              var br: BytesRef = fterms.next()
              while (br != null) {
                ppostings += ((br.utf8ToString,fterms.postings(null, PostingsEnum.POSITIONS)))
                br = fterms.next()
              }
            case qt => qp.parse(qt) match {
              case query: AutomatonQuery =>
                val fterms = new CompiledAutomaton(query.getAutomaton, null, true, Int.MaxValue, query.isAutomatonBinary).getTermsEnum(terms)
                var br: BytesRef = fterms.next()
                while (br != null) {
                  ppostings += ((br.utf8ToString,fterms.postings(null, PostingsEnum.POSITIONS)))
                  br = fterms.next()
                }
              case query: FuzzyQuery =>
                val fterms = new FuzzyTermsEnum(terms, new AttributeSource(), query.getTerm, query.getMaxEdits, query.getPrefixLength,query.getTranspositions)
                var br: BytesRef = fterms.next()
                while (br != null) {
                  ppostings += ((br.utf8ToString,fterms.postings(null, PostingsEnum.POSITIONS)))
                  br = fterms.next()
                }
              case query: TermQuery =>
                val te = terms.iterator
                if (te.seekExact(query.getTerm.bytes))
                  ppostings += ((query.getTerm.text,te.postings(null, PostingsEnum.POSITIONS)))
            }
          }
          mpqb.add(ppostings.view.map(t => new Term("content",t._1)).toArray,position)
          postings += ((increment, ppostings))
        }
        val hc = new StatsCollector(termMap,postings)
        gp.tlc.get.setCollector(hc)
        searcher(level, SumScaling.ABSOLUTE).search(mpqb.build, gp.tlc.get)
        tdf = hc.tdf
        tttf = hc.tttf
      }
      var orderedTerms = termMap.toSeq.sortBy(-_._2.ttf)
      val tterms = orderedTerms.length
      if (limit != -1) orderedTerms = orderedTerms.take(limit)
      Json.toJson("general"->Json.obj("terms"->tterms,"totalDocFreq"->tdf,"totalTermFreq"->tttf),"results"->orderedTerms.map(p => Json.obj("term" -> p._1, "docFreq" -> p._2.df, "totalTermFreq" -> p._2.ttf)))
    })
  }

}