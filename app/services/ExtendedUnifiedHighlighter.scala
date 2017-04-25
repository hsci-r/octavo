package services

import org.apache.lucene.search.uhighlight.UnifiedHighlighter
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.Query
import org.apache.lucene.search.uhighlight.DefaultPassageFormatter
import org.apache.lucene.search.uhighlight.PassageFormatter
import org.apache.lucene.search.uhighlight.Passage

class ExtendedUnifiedHighlighter(is: IndexSearcher, analyzer: Analyzer) extends UnifiedHighlighter(is, analyzer) {
  
  private val defaultPassageFormatter = new DefaultPassageFormatter()
  this.setFormatter(new PassageFormatter() {
    override def format(passages: Array[Passage], content: String): Array[String] = passages.map(passage => {
      defaultPassageFormatter.format(Array(passage), content)
    })
    
  })
  
  def highlight(field: String, query: Query, docIds: Array[Int], maxPassages: Int): Array[Array[String]] = highlightFieldsAsObjects(Array(field), query, docIds, Array(maxPassages)).get(field).map(o => o.asInstanceOf[Array[String]])
  
}