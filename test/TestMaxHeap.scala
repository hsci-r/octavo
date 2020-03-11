import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class TestMaxHeap {
  @Test
  def testMaxHeap: Unit = {
    val pq = mutable.PriorityQueue.empty[(Int,Int)]((x,y) => {
      val d1 = x._1-y._1
      if (d1!=0) d1 else x._2-y._2
    })
    for (i <- 10 to 100)
      pq += ((i,1))
    for (i <- 100 to 10)
      pq += ((i,2))
    for (i <- 10 to 100)
      pq += ((i,3))
    for (i <- 100 to 10)
      pq += ((i,4))
    for (i <- 100 to 10) {
      assertEquals((i,1),pq.dequeue())
      assertEquals((i,2),pq.dequeue())
      assertEquals((i,3),pq.dequeue())
      assertEquals((i,4),pq.dequeue())
    }
  }
}
