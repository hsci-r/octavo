package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class QueryReturnParameters()(implicit protected val request: Request[AnyContent], protected val ia: IndexAccess, protected val queryMetadata: QueryMetadata) extends ContextParameters with SortParameters {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields: Seq[String] = p.getOrElse("field", Seq.empty)
  /** return explanations for matches in search */
  val returnExplanations: Boolean = p.get("returnExplanations").exists(v => v.head=="" || v.head.toBoolean)
  private val queryScoringString = p.get("sumScaling").map(v => v.head.toUpperCase).getOrElse("TF")
  /** sum scaling to use */
  val queryScoring = QueryScoring.withName(queryScoringString)
  /** how many snippets to return at maximum for each match */
  val snippetLimit: Int = p.get("snippetLimit").map(_.head.toInt).getOrElse(0)
  /** how many results to skip */
  val offset: Int = p.get("offset").map(_.head.toInt).getOrElse(0)

  private val fullJson = Json.obj(
    "snippetLimit" -> snippetLimit,
    "offset" -> offset,
    "sumScaling"->queryScoringString,
    "fields"->fields,
    "returnExplanations"->returnExplanations
  )
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.get(pa._1 match {
    case "fields" => "field"
    case a => a
  }).isDefined))}
