package services

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search._
import org.apache.lucene.util.Bits

class NumericDocValuesWeightedMatchAllDocsQuery(field: String) extends Query {
  override def createWeight(searcher: IndexSearcher, scoreMode: ScoreMode, boost: Float): Weight = new ConstantScoreWeight(this, boost) {
    val mscore = boost
    override def toString: String = "weight(" + NumericDocValuesWeightedMatchAllDocsQuery.this + ")"
    override def scorer(context: LeafReaderContext): Scorer =
      scoreMode match {
        case ScoreMode.COMPLETE_NO_SCORES => new ConstantScoreScorer(this, mscore, scoreMode, DocIdSetIterator.all(context.reader.maxDoc))
        case ScoreMode.COMPLETE | ScoreMode.TOP_SCORES => new Scorer(this) {
          val ndv = context.reader.getNumericDocValues(field)
          val disi = DocIdSetIterator.all(context.reader.maxDoc)
          def docID: Int = disi.docID
          def iterator: org.apache.lucene.search.DocIdSetIterator = disi
          def score: Float = {
            ndv.advanceExact(disi.docID)
            ndv.longValue.toFloat
          }
          def getMaxScore(upTo: Int): Float = Float.PositiveInfinity
        }
      }
    override def bulkScorer(context: LeafReaderContext): BulkScorer = new BulkScorer() {
      val maxDoc = context.reader.maxDoc
      val ndv = context.reader.getNumericDocValues(field)
      override def score(collector: LeafCollector, acceptDocs: Bits, min: Int, maxI: Int): Int = {
        val max = Math.min(maxI, maxDoc)
        var mscore: Float = 0.0f
        var mdoc: Int = -1
        var mfreq: Int = 1
        val scorer = new Scorable {
          override def docID: Int = mdoc
          override def score: Float = mscore
        }
        collector.setScorer(scorer)
        var doc = min
        while (doc < max) {
          if (acceptDocs == null || acceptDocs.get(doc)) {
            mdoc = doc
            ndv.advanceExact(doc)
            val freq = ndv.longValue()
            mfreq = freq.toInt
            mscore = freq.toFloat
            collector.collect(doc)
          }
          doc += 1
        }
        if (max == maxDoc) DocIdSetIterator.NO_MORE_DOCS else max
      }

      override def cost(): Long = maxDoc
    }

    override def isCacheable(ctx: LeafReaderContext) = true
  }
      
  override def toString(field: String): String = "*:*"
  
  override def equals(o: Any): Boolean = sameClassAs(o)
  
  override def hashCode(): Int = classHash()
  
}