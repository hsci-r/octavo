package parameters

import java.util.concurrent.{ExecutorService, ForkJoinPool}

import com.koloboke.collect.map.LongDoubleMap
import enumeratum.{Enum, EnumEntry}
import services.TermVectors

sealed abstract class DimensionalityReduction extends EnumEntry {
  def apply(termVectors: Iterable[LongDoubleMap], rtp: TermVectorDimensionalityReductionParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]]
}

object DimensionalityReduction extends Enum[DimensionalityReduction] {
  case object CMDS extends DimensionalityReduction {
    def apply(termVectors: Iterable[LongDoubleMap], rtp: TermVectorDimensionalityReductionParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = TermVectors.mds(classical = true, termVectors, rtp)
  }
  case object SMDS extends DimensionalityReduction {
    def apply(termVectors: Iterable[LongDoubleMap], rtp: TermVectorDimensionalityReductionParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = TermVectors.mds(classical = false, termVectors, rtp)
  }
  case object TSNE extends DimensionalityReduction {
    def apply(termVectors: Iterable[LongDoubleMap], rtp: TermVectorDimensionalityReductionParameters)(implicit fjp: ForkJoinPool, ies: ExecutorService): Array[Array[Double]] = TermVectors.tsne(termVectors, rtp)
  }
  
  val values = findValues
}
