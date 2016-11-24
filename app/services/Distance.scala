package services

import com.koloboke.collect.map.ObjIntMap
import java.util.function.ObjIntConsumer
import scala.collection.JavaConverters._
import com.koloboke.collect.map.LongDoubleMap
import com.koloboke.collect.set.hash.HashLongSets
import java.util.function.LongConsumer
import java.util.function.IntConsumer
import java.util.function.DoubleConsumer


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
  
  def cosineSimilarity(x: LongDoubleMap, y: LongDoubleMap): Double = {
    //word, t1 freq, t2 freq
    val m = scala.collection.mutable.HashMap[String, (Double, Double)]()

    if (x.size==0) return 0.0
    var sum1 = 0.0
    x.values.forEach(new DoubleConsumer() {
      override def accept(freq: Double) {
        sum1 += freq
      }
    })
    if (y.size==0) return 0.0
    var sum2 = 0.0
    y.values.forEach(new DoubleConsumer() {
      override def accept(freq: Double) {
        sum2 += freq
      }
    })
    val keys = HashLongSets.newImmutableSet(x.keySet, y.keySet)
    var s1,s2,s3 = 0.0
    keys.forEach(new LongConsumer {
      override def accept(key: Long) {
        val f1 = x.getOrDefault(key, 0.0) / sum1
        val f2 = y.getOrDefault(key, 0.0) / sum2
        s1 += f1 * f2
        s2 += f1 * f1
        s3 += f2 * f2
      }
    })
    return s1 / (Math.sqrt(s2) * Math.sqrt(s3))
  }
}