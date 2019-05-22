package parameters

import org.apache.lucene.index.TermsEnum
import services.IndexAccess

trait SumScaling {
  def apply(term: Long, freq: Int): Double
}

object SumScaling {

  import IndexAccess._
  
  case object ABSOLUTE extends SumScaling {
    def apply(term: Long, freq: Int) = freq.toDouble
  }

  def TTF(it: TermsEnum): SumScaling = (term: Long, freq: Int) => freq.toDouble/totalTermFreq(it, term)

  def DF(it: TermsEnum): SumScaling = (term: Long, freq: Int) => freq.toDouble/docFreq(it,term)

  def STTF(it: TermsEnum, smoothing: Double): SumScaling = (term: Long, freq: Int) => freq.toDouble / (totalTermFreq(it, term)+smoothing)

  def SDF(it: TermsEnum, smoothing: Double): SumScaling = (term: Long, freq: Int) => freq.toDouble / (docFreq(it, term) + smoothing)

  def PMI(it: TermsEnum, smoothing: Double, queryDocFreq: Long, totalDocFreq: Long) = new SumScaling {
    val scalingTerm = totalDocFreq.toDouble / (queryDocFreq + smoothing)
    def apply(term: Long, freq: Int) = {
      // PMI = log(p(x,y)/(p(x)*p(y)))
      // p(x,y) = freq/tdf
      // p(x) = df(x)/tdf = qdf/tdf
      // p(y) = df(y)/tdf = docFreq(term)/tdf
      // (freq/tdf)/(dfx*dfy)/(tdf*tdf)
      // (freq*tdf)/(dfx*dfy)
      // (freq*tdf)/dfx/dfy
      // freq*(tdf/qdf)/dfy
      Math.log((freq*scalingTerm)/(docFreq(it,term)+smoothing))
    }
  }

  def PPMI(it: TermsEnum, smoothing: Double, queryDocFreq: Long, totalDocFreq: Long) = new SumScaling {
    val pmi = PMI(it, smoothing, queryDocFreq, totalDocFreq)
    def apply(term: Long, freq: Int) = Math.max(pmi(term, freq),0)
  }

  def get(name: String, it: TermsEnum, smoothing: Double, queryDocFreq: Long, totalDocFreq: Long): SumScaling = {
    name match {
      case "ABSOLUTE" => ABSOLUTE
      case "DF" if smoothing == 0.0 => DF(it)
      case "DF" => SDF(it, smoothing)
      case "TTF" if smoothing == 0.0 => TTF(it)
      case "TTF" => STTF(it, smoothing)
      case "PMI" => PMI(it, smoothing, queryDocFreq, totalDocFreq)
      case "PPMI" => PPMI(it, smoothing, queryDocFreq, totalDocFreq)
    }
  }

}