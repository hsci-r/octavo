package parameters

import java.text.BreakIterator

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.IndexSearcher
import play.api.libs.json.Json
import play.api.mvc.{AnyContent, Request}
import services.{ExtendedUnifiedHighlighter, IndexAccess}

class ContextParameters()(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)

  /** context level when returning matches */
  val contextLevel = p.get("contextLevel").map(v => ContextLevel.withName(v.head.toUpperCase)).getOrElse(ContextLevel.SENTENCE)
  /** how much to expand returned match context */
  val contextExpandLeft = p.get("contextExpandLeft").map(v => v.head.toInt).getOrElse(0)
  val contextExpandRight = p.get("contextExpandRight").map(v => v.head.toInt).getOrElse(0)
  def highlighter(is: IndexSearcher, analyzer: Analyzer): ExtendedUnifiedHighlighter = new ExtendedUnifiedHighlighter(is, analyzer) {
    override def getBreakIterator(field: String): BreakIterator = contextLevel(contextExpandLeft,contextExpandRight)
  }
  private val myJson = Json.obj(
    "contextLevel" -> contextLevel.entryName,
    "contextExpandLeft" -> contextExpandLeft,
    "contextExpandRight" -> contextExpandRight
  )

  def toJson = myJson
  queryMetadata.json = queryMetadata.json ++ myJson
}
