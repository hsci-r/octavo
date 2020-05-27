package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class QueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) {
  protected val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val defaultLevel: String = p.get(prefix+"level"+suffix).orElse(p.get(prefix+"defaultLevel"+suffix)).map(_.head).getOrElse(ia.indexMetadata.defaultLevel.id.toUpperCase)
  private val queryS: Option[String] = p.get(prefix+"query"+suffix).map(_.head)
  val query: Option[String] = queryS.map(v => if (!v.startsWith("<")) "<"+defaultLevel+"§"+v+"§"+defaultLevel+">" else v)
  def requiredQuery: String = query.getOrElse("<"+defaultLevel+"§§"+defaultLevel+">")
  private val fullJson = Json.obj(
    prefix+"query"+suffix->queryS,
    prefix+"level"+suffix->defaultLevel
  )
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.contains(pa._1)))

}