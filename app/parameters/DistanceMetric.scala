package parameters

import enumeratum.EnumEntry
import com.koloboke.collect.map.LongDoubleMap
import enumeratum.Enum
import services.Distance

sealed abstract class DistanceMetric extends EnumEntry {
  def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double
  def apply(t1: LongDoubleMap, t2: LongDoubleMap): Double = 1.0 - similarity(t1, t2)
}

object DistanceMetric extends Enum[DistanceMetric] {
  case object COSINE extends DistanceMetric {
    def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double = Distance.cosineSimilarity(t1, t2)
  }
  case object DICE extends DistanceMetric {
    def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double = Distance.diceSimilarity(t1, t2)
  } 
  case object JACCARD extends DistanceMetric {
    def similarity(t1: LongDoubleMap, t2: LongDoubleMap): Double = Distance.jaccardSimilarity(t1, t2)
  }
  
  val values = findValues
}
