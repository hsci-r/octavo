package parameters

import enumeratum.EnumEntry
import com.koloboke.collect.map.LongDoubleMap
import enumeratum.Enum
import services.Distance

sealed abstract class DistanceMetric extends EnumEntry {
  def apply(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double = {
    tvp.normalization(t1)
    tvp.normalization(t2)
    distance(t1, t2, tvp)
  }
  protected def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double
}

object DistanceMetric extends Enum[DistanceMetric] {
  case object COSINE extends DistanceMetric {
    protected def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double = 1.0 - Distance.cosineSimilarity(t1, t2, tvp.filtering)
  }
  case object DICE extends DistanceMetric {
    protected def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double = 1.0 - Distance.diceSimilarity(t1, t2)
  } 
  case object JACCARD extends DistanceMetric {
    protected def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double = 1.0 - Distance.jaccardSimilarity(t1, t2)
  }
  case object EUCLIDEAN extends DistanceMetric {
    protected def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double = Distance.euclideanDistance(t1, t2, tvp.filtering)
  }
  case object MANHATTAN extends DistanceMetric {
    protected def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: TermVectorDistanceCalculationParameters): Double = Distance.manhattanDistance(t1, t2, tvp.filtering)
  }
  
  val values = findValues
}
