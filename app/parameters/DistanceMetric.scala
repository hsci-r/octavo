package parameters

import enumeratum.EnumEntry
import com.koloboke.collect.map.LongDoubleMap
import enumeratum.Enum
import services.Distance

sealed abstract class DistanceMetric extends EnumEntry {
  def apply(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double = {
    if (tvp.center) {
      Distance.center(t1)
      Distance.center(t2)
    }
    if (tvp.normalize) {
      Distance.normalize(t1)
      Distance.normalize(t2)
    }
    distance(t1, t2, tvp)
  }
  def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double
}

object DistanceMetric extends Enum[DistanceMetric] {
  case object COSINE extends DistanceMetric {
    def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double = 1.0 - Distance.cosineSimilarity(t1, t2, tvp.onlyCommon)
  }
  case object DICE extends DistanceMetric {
    def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double = 1.0 - Distance.diceSimilarity(t1, t2)
  } 
  case object JACCARD extends DistanceMetric {
    def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double = 1.0 - Distance.jaccardSimilarity(t1, t2)
  }
  case object EUCLIDEAN extends DistanceMetric {
    def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double = Distance.euclideanDistance(t1, t2, tvp.onlyCommon)
  }
  case object MANHATTAN extends DistanceMetric {
    def distance(t1: LongDoubleMap, t2: LongDoubleMap, tvp: AggregateTermVectorProcessingParameters): Double = Distance.manhattanDistance(t1, t2, tvp.onlyCommon)
  }
  
  val values = findValues
}
