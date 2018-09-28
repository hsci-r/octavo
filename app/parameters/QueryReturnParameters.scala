package parameters

import play.api.libs.json.Json
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class QueryReturnParameters()(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) extends ContextParameters {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields: Seq[String] = p.getOrElse("field", Seq.empty)
  /** return explanations for matches in search */
  val returnExplanations: Boolean = p.get("returnExplanations").exists(v => v.head=="" || v.head.toBoolean)
  private val sumScalingStringOpt = p.get("sumScaling").map(v => v.head.toUpperCase)
  private val sumScalingString = sumScalingStringOpt.getOrElse("TTF")
  /** sum scaling to use */
  val sumScaling = SumScaling.get(sumScalingString, 0.0)
  /** how many snippets to return at maximum for each match */
  val snippetLimit: Int = p.get("snippetLimit").map(_.head.toInt).getOrElse(0)
  /** how many results to skip */
  val offset: Int = p.get("offset").map(_.head.toInt).getOrElse(0)
  private val sortFields = p.getOrElse("sortField",Seq.empty)
  private val sortDirections = p.getOrElse("sortDirection",Seq.empty).map(sd => sd match {
    case "A" | "a" => SortDirection.ASC
    case "D" | "d" => SortDirection.DESC
    case _ => SortDirection.withName(sd.toUpperCase)})
  val sorts = sortFields.zipAll(sortDirections,ia.indexMetadata.contentField,SortDirection.ASC)

  private val myJson = Json.obj(
    "snippetLimit" -> snippetLimit,
    "offset" -> offset,
    "sumScaling"->sumScalingString,
    "fields"->fields,
    "sortFields"->sortFields,
    "sortDirections"->sortDirections,
    "returnExplanations"->returnExplanations
  )
  override def toJson = super.toJson ++ myJson
  queryMetadata.json = queryMetadata.json ++ myJson
}
