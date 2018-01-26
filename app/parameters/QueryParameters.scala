package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class QueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) {
  protected val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val query: Option[String] = p.get(prefix+"query"+suffix).map(v => if (!v.head.startsWith("<")) "<"+ia.indexMetadata.defaultLevel.id+"§"+v.head+"§"+ia.indexMetadata.defaultLevel.id+">" else v.head)
  def requiredQuery: String = query.getOrElse("<"+ia.indexMetadata.defaultLevel.id+"§§"+ia.indexMetadata.defaultLevel.id+">")
  def toJson: JsObject = Json.obj(
    prefix+"query"+suffix->query
  )
  queryMetadata.json = queryMetadata.json ++ toJson
}