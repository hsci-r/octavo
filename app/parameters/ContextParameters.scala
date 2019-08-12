package parameters

import java.text.BreakIterator

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.IndexSearcher
import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.{ExtendedUnifiedHighlighter, IndexAccess, OffsetSearchType}

class ContextParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) extends LimitParameters(prefix,suffix) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)

  /** context level when returning matches */
  val contextLevel = p.get("contextLevel").map(v => ContextLevel.withName(v.head.toUpperCase)).getOrElse(ContextLevel.SENTENCE)
  /** how much to expand returned match context */
  val contextExpandLeft = p.get("contextExpandLeft").map(v => v.head.toInt).getOrElse(0)
  val contextExpandRight = p.get("contextExpandRight").map(v => v.head.toInt).getOrElse(0)

  val offsetData: Boolean = p.get("offsetData").exists(v => v.head=="" || v.head.toBoolean)
  val startOffsetSearchType = p.get("startOffsetSearchType").map(v => OffsetSearchType.withName(v.head.toUpperCase)).getOrElse(contextLevel.defaultStartSearchType)
  val endOffsetSearchType = p.get("endOffsetSearchType").map(v => OffsetSearchType.withName(v.head.toUpperCase)).getOrElse(contextLevel.defaultEndSearchType)
  val matchOffsetSearchType = p.get("matchOffsetSearchType").map(v => OffsetSearchType.withName(v.head.toUpperCase)).getOrElse(OffsetSearchType.EXACT)

  def highlighter(is: IndexSearcher, analyzer: Analyzer, matchFullSpans: Boolean = false): ExtendedUnifiedHighlighter = new ExtendedUnifiedHighlighter(is, analyzer,matchFullSpans) {
    override def getBreakIterator(field: String): BreakIterator = contextLevel(contextExpandLeft,contextExpandRight)
  }
  private val fullJson = Json.obj(
    "contextLevel" -> contextLevel.entryName,
    "contextExpandLeft" -> contextExpandLeft,
    "contextExpandRight" -> contextExpandRight,
    "offsetData" -> offsetData,
    "startOffsetSearchType" ->  startOffsetSearchType,
    "endOffsetSearchType" -> endOffsetSearchType,
    "matchOffsetSearchType" -> matchOffsetSearchType
  )

  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson

  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.get(pa._1).isDefined))

}
