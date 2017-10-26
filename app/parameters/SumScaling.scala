package parameters

import org.apache.lucene.index.IndexReader
import services.IndexAccess

trait SumScaling {
  def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess): Double
}

object SumScaling {
  
  case object ABSOLUTE extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble
  }
  case object DF extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble/ia.docFreq(ir,term)
  }
  case object TTF extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble/ia.totalTermFreq(ir, term)
  }
  
  def STTF(smoothing: Double): SumScaling = new SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble/(ia.totalTermFreq(ir, term)+smoothing)
  }

  def SDF(smoothing: Double): SumScaling = new SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble/(ia.docFreq(ir,term)+smoothing)
  }
  
  def get(name: String, smoothing: Double): SumScaling = {
    name match {
      case "ABSOLUTE" => ABSOLUTE
      case "DF" if smoothing == 0.0 => DF
      case "DF" => SDF(smoothing)
      case "TTF" if smoothing == 0.0 => TTF
      case "TTF" => STTF(smoothing)
    }
  }

}