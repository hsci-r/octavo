package services

import com.koloboke.collect.map.ObjIntMap
import java.util.function.ObjIntConsumer
import scala.collection.JavaConverters._
import com.koloboke.collect.map.LongDoubleMap
import com.koloboke.collect.set.hash.HashLongSets
import java.util.function.LongConsumer
import java.util.function.IntConsumer
import java.util.function.DoubleConsumer
import com.koloboke.function.LongDoubleToDoubleFunction

import enumeratum.EnumEntry
import enumeratum.Enum
import com.koloboke.collect.set.LongSet

sealed abstract class Filtering extends EnumEntry {
  def apply(x: LongSet, y: LongSet): LongSet 
}

object Filtering extends Enum[Filtering] {
  case object BOTH extends Filtering {
    def apply(xo: LongSet, yo: LongSet): LongSet = {
      val (x,y) = if (xo.size < yo.size) (xo,yo) else (yo,xo) 
      val ret = HashLongSets.newUpdatableSet(x.size)
      x.forEach(new LongConsumer() {
        override def accept(key: Long) = if (y.contains(key)) ret.add(key)
      })
      ret
    }
  }
  case object EITHER extends Filtering {
    def apply(x: LongSet, y: LongSet): LongSet = HashLongSets.newImmutableSet(x,y)
  }
  case object LEFT extends Filtering {
    def apply(x: LongSet, y: LongSet): LongSet = x
  }
  case object RIGHT extends Filtering {
    def apply(x: LongSet, y: LongSet): LongSet = y
  }
  
  val values = findValues
}

sealed abstract class Normalization extends EnumEntry {
  def apply(x: LongDoubleMap): Unit 
}

object Normalization extends Enum[Normalization] {
  case object NONE extends Normalization {
    def apply(x: LongDoubleMap): Unit = {
    }
  }
  case object NORMALIZE extends Normalization {
    def apply(x: LongDoubleMap): Unit = {
      Distance.normalize(x)
    }
  }
  case object CENTERNORM extends Normalization {
    def apply(x: LongDoubleMap): Unit = {
      Distance.normalize(x)
      Distance.center(x)
    }
  }
  
  val values = findValues
}

object Distance {
  
  def jaccardSimilarity(x: LongDoubleMap, y: LongDoubleMap): Double = {
    var nom = 0.0
    var denom = 0.0
    val keys = HashLongSets.newImmutableSet(x.keySet, y.keySet)
    keys.forEach(new LongConsumer() {
      override def accept(key: Long) {
        nom+=math.min(x.getOrDefault(key,0.0),y.getOrDefault(key,0.0))
        denom+=math.max(x.getOrDefault(key,0.0),y.getOrDefault(key,0.0))
      }
    })
    return nom.toDouble/denom
  }
  
  def diceSimilarity(x: LongDoubleMap, y: LongDoubleMap): Double = {
    var nom = 0.0
    var denom = 0.0
    val keys = HashLongSets.newImmutableSet(x.keySet, y.keySet)
    keys.forEach(new LongConsumer() {
      override def accept(key: Long) {
        nom+=math.min(x.getOrDefault(key,0.0),y.getOrDefault(key,0.0))
        denom+=x.getOrDefault(key,0.0)+y.getOrDefault(key,0.0)
      }
    })
    return (nom*2.0)/denom
  }
  
  def center(x: LongDoubleMap): Unit = {
    if (x.size == 0) return
    var sum = 0.0
    x.values.forEach(new DoubleConsumer() {
      override def accept(freq: Double) {
        sum += freq
      }
    })
    val mean = sum / x.size
    x.replaceAll(new LongDoubleToDoubleFunction() {
      override def applyAsDouble(key: Long, value: Double): Double = value - mean
    })
  }
  
  def normalize(x: LongDoubleMap): Unit = {
    if (x.size == 0) return
    var sum = 0.0
    x.values.forEach(new DoubleConsumer() {
      override def accept(freq: Double) {
        sum += freq*freq
      }
    })
    val length = math.sqrt(sum)
    x.replaceAll(new LongDoubleToDoubleFunction() {
      override def applyAsDouble(key: Long, value: Double): Double = value / length
    })
  }
    
  def euclideanDistance(x: LongDoubleMap, y: LongDoubleMap, filtering: Filtering): Double = {
    if (x.size==0) return Double.NaN
    if (y.size==0) return Double.NaN
    val keys = filtering(x.keySet, y.keySet)
    var sum = 0.0
    keys.forEach(new LongConsumer {
      override def accept(key: Long) {
        val f1 = x.getOrDefault(key, 0.0)
        val f2 = y.getOrDefault(key, 0.0)
        val diff = f1 - f2
        sum += diff*diff 
      }
    })
    return math.sqrt(sum)
  }
  
  def manhattanDistance(x: LongDoubleMap, y: LongDoubleMap, filtering: Filtering): Double = {
    if (x.size==0) return Double.NaN
    if (y.size==0) return Double.NaN
    val keys = filtering(x.keySet, y.keySet)
    var sum = 0.0
    keys.forEach(new LongConsumer {
      override def accept(key: Long) {
        val f1 = x.getOrDefault(key, 0.0)
        val f2 = y.getOrDefault(key, 0.0)
        sum += math.abs(f1 - f2)
      }
    })
    return math.sqrt(sum)
  }
  
  def cosineSimilarity(x: LongDoubleMap, y: LongDoubleMap, filtering: Filtering): Double = {
    val keys = filtering(x.keySet, y.keySet)
    var s1,s2,s3 = 0.0
    keys.forEach(new LongConsumer {
      override def accept(key: Long) {
        val f1 = x.getOrDefault(key, 0.0)
        val f2 = y.getOrDefault(key, 0.0)
        s1 += f1 * f2
        s2 += f1 * f1
        s3 += f2 * f2
      }
    })
    if (s2 == 0.0 || s3 == 0.0) return 0.0
    return s1 / (math.sqrt(s2) * math.sqrt(s3))
  }
}