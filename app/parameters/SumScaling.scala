package parameters

import enumeratum.EnumEntry
import org.apache.lucene.index.IndexReader
import enumeratum.Enum
import services.IndexAccess

sealed trait SumScaling extends EnumEntry {
  def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess): Double
}

object SumScaling extends Enum[SumScaling] {
  
  case object ABSOLUTE extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble
  }
  case object DF extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble/ia.docFreq(ir,term)
  }
  case object TTF extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int)(implicit ia: IndexAccess) = freq.toDouble/ia.totalTermFreq(ir, term)
  }
  
  val values = findValues
}