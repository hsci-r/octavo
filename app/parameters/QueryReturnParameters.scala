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
  private val sortFields = p.getOrElse("sort",Seq.empty)
  private val sortDirections = p.getOrElse("sortDirection",Seq.empty).map {
    case "A" | "a" => SortDirection.ASC
    case "D" | "d" => SortDirection.DESC
    case sd => SortDirection.withName(sd.toUpperCase)
  }
  private val sortCaseInsensitivities = p.getOrElse("sortCaseInsensitive",Seq.empty).map(sd => sd.toLowerCase match {
    case "i" | "insensitive" => true
    case "s" | "sensitive" => false
  })
  val sorts = sortFields.zipAll(sortDirections,ia.indexMetadata.contentField,SortDirection.ASC).zipAll(sortCaseInsensitivities,null,false).map(p => (p._1._1,p._1._2,p._2))

  private val myJson = Json.obj(
    "snippetLimit" -> snippetLimit,
    "offset" -> offset,
    "sumScaling"->sumScalingString,
    "fields"->fields,
    "sorts"->sortFields,
    "sortDirections"->sortDirections,
    "sortCaseInsensitivities"->sortCaseInsensitivities,
    "returnExplanations"->returnExplanations
  )
  override def toJson = super.toJson ++ myJson
  queryMetadata.json = queryMetadata.json ++ myJson
}
