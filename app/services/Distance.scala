package services

import com.koloboke.collect.map.ObjIntMap
import java.util.function.ObjIntConsumer
import scala.collection.JavaConverters._


object Distance {
  
  def jaccardSimilarity(x: ObjIntMap[String], y: ObjIntMap[String]): Double = {
    var nom = 0
    var denom = 0
    for (key <- x.keySet.asScala ++ y.keySet.asScala) {
      nom+=math.min(x.getOrDefault(key,0),y.getOrDefault(key,0))
      denom+=math.max(x.getOrDefault(key,0),y.getOrDefault(key,0))
    }
    return nom.toDouble/denom
  }
  
  def diceSimilarity(x: ObjIntMap[String], y: ObjIntMap[String]): Double = {
    var nom = 0
    var denom = 0
    for (key <- x.keySet.asScala ++ y.keySet.asScala) {
      nom+=math.min(x.getOrDefault(key,0),y.getOrDefault(key,0))
      denom+=x.getOrDefault(key,0)+y.getOrDefault(key,0)
    }
    return (nom*2).toDouble/denom
  }
  
  def cosineSimilarity(t1: ObjIntMap[String], t2: ObjIntMap[String]): Double = {
     //word, t1 freq, t2 freq
     val m = scala.collection.mutable.HashMap[String, (Int, Int)]()

     var sum1 = 0 
     t1.forEach(new ObjIntConsumer[String] {
       override def accept(word: String, freq: Int): Unit = {
         m += word -> (freq, 0)
         sum1 += freq
       }
       
     })
     var sum2 = 0
     t2.forEach(new ObjIntConsumer[String] {
       override def accept(word: String, freq: Int): Unit = {
         m.get(word) match {
             case Some((freq1, _)) => m += word ->(freq1, freq)
             case None => m += word ->(0, freq)
         }
         sum2 += freq
       }
     })

     val (p1, p2, p3) = m.foldLeft((0d, 0d, 0d)) {case ((s1, s2, s3), e) =>
         val fs = e._2
         val f1 = fs._1.toDouble / sum1
         val f2 = fs._2.toDouble / sum2
         (s1 + f1 * f2, s2 + f1 * f1, s3 + f2 * f2)
     }

     val cos = p1 / (Math.sqrt(p2) * Math.sqrt(p3))
     cos
   }
}