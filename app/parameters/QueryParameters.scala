package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class QueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) {
  protected val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val defaultLevel: String = p.get(prefix+"level"+suffix).orElse(p.get(prefix+"defaultLevel"+suffix)).map(_.head).getOrElse(ia.indexMetadata.defaultLevel.id.toUpperCase)
  val query: Option[String] = p.get(prefix+"query"+suffix).map(v => if (!v.head.startsWith("<")) "<"+defaultLevel+"§"+v.head+"§"+defaultLevel+">" else v.head)
  def requiredQuery: String = query.getOrElse("<"+defaultLevel+"§§"+defaultLevel+">")
  def toJson: JsObject = Json.obj(
    prefix+"query"+suffix->query
  )
  queryMetadata.json = queryMetadata.json ++ toJson
}