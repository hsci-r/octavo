package controllers

import javax.inject.{Inject, Singleton}
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, PositionIncrementAttribute}
import org.apache.lucene.index.{PostingsEnum, Term, Terms}
import org.apache.lucene.search.{AutomatonQuery, BooleanQuery, FuzzyQuery, FuzzyTermsEnum, MultiPhraseQuery, Query, ScoreMode, SimpleCollector, TermInSetQuery, TermQuery}
import org.apache.lucene.util.automaton.CompiledAutomaton
import org.apache.lucene.util.{AttributeSource, BytesRef}
import play.api.libs.json.{JsObject, Json}
import services.IndexAccessProvider

import scala.jdk.CollectionConverters._
import parameters.{GeneralParameters, QueryMetadata, QueryScoring, SortDirection}

import scala.collection.Searching.Found
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object SortBy extends Enumeration {
  val TERM, TTF, TDF = Value
}

@Singleton
class SimilarTermsController  @Inject() (implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  /*private def permutations[A](a: Iterable[A]*): Iterator[Seq[A]] = a.foldLeft(Iterator(Seq.empty[A])) {
    (acc: Iterator[Seq[A]], next: Iterable[A]) => acc.flatMap { combo: Seq[A] => next.map { num => combo :+ num } }
  }*/

  class Stats(var df: Long = 0,var ttf: Long = 0) {}

  class StatsCollector(val termMap: mutable.HashMap[String, Stats], val apostings: ArrayBuffer[(Int,ArrayBuffer[(String,PostingsEnum)])]) extends SimpleCollector {

    var tdf = 0L
    var tttf = 0L

    override def scoreMode = ScoreMode.COMPLETE_NO_SCORES

    val docMatches = new mutable.HashMap[String,Stats]()

    def check(sterm: String, position: Int, rPositions: Iterable[(Int,ArrayBuffer[(String,ArrayBuffer[Int])])]): Unit = {
      val tail = rPositions.tail
      val (pi,termPositions) = rPositions.head
      for ((term,positions)<-termPositions) {
        positions.search(position + pi) match {
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

    override def collect(doc: Int): Unit = {
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

  val ExtendedFuzzyQuery = "([^|]*)~([12]):([0-9]*):?(.*)".r

  import services.IndexAccess._

  private def toIterator(terms: Terms, q: Query): Iterator[(BytesRef,Int,Long)] = q match {
    case query: TermInSetQuery =>
      val te = terms.iterator
      query.getTermData.iterator.asBytesRefIterator.map(b => {
        te.seekExact(b)
        (b,te.docFreq,te.totalTermFreq)
      })
    case query: AutomatonQuery => new CompiledAutomaton(query.getAutomaton, null, true, Int.MaxValue, query.isAutomatonBinary).getTermsEnum(terms).asBytesRefAndDocFreqAndTotalTermFreqIterator
    case query: TermQuery => val te = terms.iterator
      te.seekExact(query.getTerm.bytes)
      Iterator((query.getTerm.bytes,te.docFreq,te.totalTermFreq))
    case query: BooleanQuery =>
      val col = new mutable.HashSet[(BytesRef,Int,Long)]
      for (c <- query.clauses.asScala; i <- toIterator(terms, c.getQuery)) col += ((BytesRef.deepCopyOf(i._1),i._2,i._3))
      col.iterator
    case query: FuzzyQuery =>
      new FuzzyTermsEnum(terms, query.getTerm, query.getMaxEdits, query.getPrefixLength,query.getTranspositions).asBytesRefAndDocFreqAndTotalTermFreqIterator
  }
  
  // get terms lexically similar to a query term - used in topic definition to get past OCR errors
  def similarTerms(index: String, levelO: Option[String], query: String, limit: Int = 20, offset: Int = 0, sortS: String = "TDF", sortDirectionS: String = "D") = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val level = levelO.getOrElse(indexMetadata.defaultLevel.id)
    val sortDirection = sortDirectionS match {
      case "A" | "a" => SortDirection.ASC
      case "D" | "d" => SortDirection.DESC
      case sd => SortDirection.withName(sd.toUpperCase)
    }
    val sort = SortBy.withName(sortS.toUpperCase)
    val fullJson = Json.obj(
      "query" -> query,
      "sort" -> sort,
      "sortDirection" -> sortDirection,
      "level" -> level,
      "offset"->offset,
      "limit"->limit)
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    implicit val qm = new QueryMetadata(JsObject(fullJson.fields.filter(pa => p.contains(pa._1))),fullJson)
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
      var tdf = 0L
      var tttf = 0L
      val termMap = new mutable.HashMap[String, Stats]()
      if (parts.length == 1) {
        val qt = parts(0)._2
        for ((br, docFreq, termFreq) <- qt match {
          case ExtendedFuzzyQuery(rqt,maxEdits,prefixLength,transpositions) => new FuzzyTermsEnum(terms, new Term(indexMetadata.contentField, rqt), maxEdits.toInt, if (prefixLength=="") 0 else prefixLength.toInt, transpositions != "" && transpositions.toBoolean ).asBytesRefAndDocFreqAndTotalTermFreqIterator
          case _ => toIterator(terms, qp.parse(qt)) }
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
              val fterms = new FuzzyTermsEnum(terms, new Term(indexMetadata.contentField, rqt), maxEdits.toInt, if (prefixLength=="") 0 else prefixLength.toInt, transpositions != "" && transpositions.toBoolean )
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
                val fterms = new FuzzyTermsEnum(terms, query.getTerm, query.getMaxEdits, query.getPrefixLength,query.getTranspositions)
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
        searcher(level, QueryScoring.NONE).search(mpqb.build, gp.tlc.get)
        tdf = hc.tdf
        tttf = hc.tttf
      }
      var orderedTerms = termMap.toSeq.sortWith((x,y) => sort match {
          case SortBy.TERM => sortDirection match {
            case SortDirection.ASC => x._1<y._1
            case SortDirection.DESC => x._1>y._1
          }
          case SortBy.TDF => sortDirection match {
            case SortDirection.ASC => x._2.df<y._2.df
            case SortDirection.DESC => x._2.df>y._2.df
          }
          case SortBy.TTF => sortDirection match {
            case SortDirection.ASC => x._2.ttf<y._2.ttf
            case SortDirection.DESC => x._2.ttf>y._2.ttf
          }
        })
      val tterms = orderedTerms.length
      orderedTerms = orderedTerms.drop(offset)
      if (limit != -1) orderedTerms = orderedTerms.take(limit)
      Left(Json.obj("general"->Json.obj("terms"->tterms,"totalDocFreq"->tdf,"totalTermFreq"->tttf),"results"->orderedTerms.map(p => Json.obj("term" -> p._1, "totalDocFreq" -> p._2.df, "totalTermFreq" -> p._2.ttf))))
    })
  }

}