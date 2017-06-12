package parameters

import enumeratum.EnumEntry
import com.koloboke.collect.map.LongDoubleMap
import enumeratum.Enum
import services.Distance
import services.TermVectors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ExecutorService

sealed abstract class DimensionalityReduction extends EnumEntry {
  def apply(termVectors: Iterable[LongDoubleMap], rtp: AggregateTermVectorProcessingParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] 
}

object DimensionalityReduction extends Enum[DimensionalityReduction] {
  case object CMDS extends DimensionalityReduction {
    def apply(termVectors: Iterable[LongDoubleMap], rtp: AggregateTermVectorProcessingParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = TermVectors.mds(true, termVectors, rtp)
  }
  case object SMDS extends DimensionalityReduction {
    def apply(termVectors: Iterable[LongDoubleMap], rtp: AggregateTermVectorProcessingParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = TermVectors.mds(false, termVectors, rtp)
  }
  case object TSNE extends DimensionalityReduction {
    def apply(termVectors: Iterable[LongDoubleMap], rtp: AggregateTermVectorProcessingParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = TermVectors.tsne(termVectors, rtp)
  }
  
  val values = findValues
}
