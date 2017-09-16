import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import fi.seco.lucene.MorphologicalAnalyzer
import java.util.Locale
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import services.Distance
import com.koloboke.collect.map.hash.HashLongDoubleMaps
import org.junit.runner.RunWith
import com.novocode.junit.JUnitRunner
import services.Filtering

class TestDistances {
  
  @Test
  def testCosineDistance {
    val x = HashLongDoubleMaps.newMutableMapOf(1, 3.0, 2, 1.0, 3, 2.0, 4, 2.0, 5, 1.0)
    val y = HashLongDoubleMaps.newMutableMapOf(1, 3.0,         3, 2.0, 4, 1.0,         6, 1.0, 7, 5.0)
    val z = HashLongDoubleMaps.newMutableMapOf(6, 1.0, 7, 5.0)
    val z2 = HashLongDoubleMaps.newMutableMapOf(6, 3.0, 7, 7.0)
    val z3 = HashLongDoubleMaps.newMutableMapOf(6, 2.0, 7, 10.0)
    val z4 = HashLongDoubleMaps.newMutableMapOf(6, 2.0, 7, 10.0)
    val z5 = HashLongDoubleMaps.newMutableMapOf(6, 1.0, 7, 5.0)
    val z6 = HashLongDoubleMaps.newMutableMapOf(6, 3.0, 7, 7.0)
    assertEquals(1.0, Distance.cosineSimilarity(z, z3, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(x, x, Filtering.EITHER), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(x, x, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(y, y, Filtering.EITHER), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(y, y, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(z, z, Filtering.EITHER), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(z, z, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(y, z, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(y, z, Filtering.RIGHT), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.EITHER), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.BOTH), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.LEFT), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.RIGHT), 0.000001)
    Distance.center(x)
    assertEquals(1.0, Distance.cosineSimilarity(x, x, Filtering.EITHER), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(x, x, Filtering.BOTH), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.EITHER), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.BOTH), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.LEFT), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.RIGHT), 0.000001)
    Distance.center(y)
    Distance.center(z)
    assertEquals(1.0, Distance.cosineSimilarity(y, y, Filtering.EITHER), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(y, y, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(z, z, Filtering.EITHER), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(z, z, Filtering.BOTH), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.EITHER), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.BOTH), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.LEFT), 0.000001)
    assertEquals(0.0, Distance.cosineSimilarity(x, z, Filtering.RIGHT), 0.000001)
    Distance.center(z2)
    assertEquals(1.0, Distance.cosineSimilarity(z, z2, Filtering.BOTH), 0.000001)
    assertEquals(1.0, Distance.cosineSimilarity(z4, z5, Filtering.BOTH), 0.000001)
    Distance.normalize(z4)
    assertEquals(1.0, Distance.cosineSimilarity(z4, z5, Filtering.BOTH), 0.000001)
    Distance.normalize(z5)
    assertEquals(1.0, Distance.cosineSimilarity(z4, z5, Filtering.BOTH), 0.000001)
    Distance.center(z4)
    Distance.center(z5)
    assertEquals(1.0, Distance.cosineSimilarity(z4, z5, Filtering.BOTH), 0.000001)
    Distance.normalize(z6)
    Distance.center(z6)
    assertEquals(1.0, Distance.cosineSimilarity(z4, z6, Filtering.BOTH), 0.000001)
 }
  
}