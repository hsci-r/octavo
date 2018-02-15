package services

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.uhighlight.{DefaultPassageFormatter, Passage, UnifiedHighlighter}
import org.apache.lucene.search.{IndexSearcher, Query}

class ExtendedUnifiedHighlighter(is: IndexSearcher, analyzer: Analyzer) extends UnifiedHighlighter(is, analyzer) {
  
  private val defaultPassageFormatter = new DefaultPassageFormatter()
  this.setFormatter((passages: Array[Passage], content: String) => passages.map(passage => {
    defaultPassageFormatter.format(Array(passage), content)
  }))
  
  def highlight(field: String, query: Query, docIds: Array[Int], maxPassages: Int): Array[Array[String]] = {
    highlightFieldsAsObjects(Array(field), query, docIds, Array(maxPassages)).get(field).map(o => o.asInstanceOf[Array[String]])
  }

  setMaxLength(Int.MaxValue - 1)
}
