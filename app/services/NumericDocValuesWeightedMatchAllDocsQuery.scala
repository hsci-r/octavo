package services

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.util.Bits
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.search.Query
import org.apache.lucene.search.Weight
import org.apache.lucene.search.ConstantScoreWeight
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.ConstantScoreScorer
import org.apache.lucene.search.Scorer
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.search.BulkScorer
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.Scorer.ChildScorer

class NumericDocValuesWeightedMatchAllDocsQuery(field: String) extends Query {
  override def createWeight(searcher: IndexSearcher, needsScores: Boolean): Weight = {
    return new ConstantScoreWeight(this) {
      val mscore = super.score()
      override def toString(): String = "weight(" + NumericDocValuesWeightedMatchAllDocsQuery.this + ")"
      override def scorer(context: LeafReaderContext): Scorer = 
        if (!needsScores) new ConstantScoreScorer(this, mscore, DocIdSetIterator.all(context.reader.maxDoc))
        else new Scorer(this) {
          val ndv = context.reader.getNumericDocValues(field)
          val disi = DocIdSetIterator.all(context.reader.maxDoc)  
          def docID(): Int = disi.docID   
          def freq(): Int = ndv.get(disi.docID).toInt
          def iterator(): org.apache.lucene.search.DocIdSetIterator = disi  
          def score(): Float = ndv.get(disi.docID).toFloat
        }
      override def bulkScorer(context: LeafReaderContext): BulkScorer = {
        return new BulkScorer() {
          val maxDoc = context.reader.maxDoc
          val ndv = context.reader.getNumericDocValues(field)
          override def score(collector: LeafCollector, acceptDocs: Bits, min: Int, maxI: Int): Int = {
            val max = Math.min(maxI, maxDoc)
            var mscore: Float = 0.0f
            var mdoc: Int = -1
            var mfreq: Int = 1
            val scorer = new Scorer(null) {
              override def docID(): Int = mdoc
              override def freq(): Int = mfreq
              override def score(): Float = mscore
              override def iterator(): DocIdSetIterator = throw new UnsupportedOperationException()
              override def getWeight(): Weight = throw new UnsupportedOperationException()
              override def getChildren(): java.util.Collection[ChildScorer] = throw new UnsupportedOperationException()
            }
            collector.setScorer(scorer)
            var doc = min
            while (doc < max) {
              if (acceptDocs == null || acceptDocs.get(doc)) {
                mdoc = doc;
                val freq = ndv.get(doc)
                mfreq = freq.toInt
                mscore = freq.toFloat
                collector.collect(doc)
              }
              doc += 1
            }
            return if (max == maxDoc) DocIdSetIterator.NO_MORE_DOCS else max
          }
          
          override def cost(): Long = maxDoc
        }
      }
    }
  }
      
  override def toString(field: String): String = "*:*"
  
  override def equals(o: Any): Boolean = sameClassAs(o)
  
  override def hashCode(): Int = classHash()
  
}