package parameters

import enumeratum.EnumEntry
import org.apache.lucene.index.IndexReader
import enumeratum.Enum

sealed trait SumScaling extends EnumEntry {
  def apply(ir: IndexReader, term: Long, freq: Int): Double
}

object SumScaling extends Enum[SumScaling] {
  
  import services.IndexAccess.{totalTermFreq,docFreq}
  
  case object ABSOLUTE extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int) = freq.toDouble
  }
  case object DF extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int) = freq.toDouble/docFreq(ir,term)
  }
  case object TTF extends SumScaling {
    def apply(ir: IndexReader, term: Long, freq: Int) = freq.toDouble/totalTermFreq(ir, term)
  }
  
  val values = findValues
}