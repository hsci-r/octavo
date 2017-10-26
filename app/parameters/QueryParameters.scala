package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

case class QueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess) {
  protected val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val query: Option[String] = p.get(prefix+"query"+suffix).map(v => if (!v.head.startsWith("<")) "<"+ia.indexMetadata.defaultLevel.id+"§"+v.head+"§"+ia.indexMetadata.defaultLevel.id+">" else v.head)
  def requiredQuery: String = query.getOrElse("<"+ia.indexMetadata.defaultLevel.id+"§§"+ia.indexMetadata.defaultLevel.id+">")
  /** minimum query score (by default term match frequency) for doc to be included in query results */
  val minScore: Float = p.get(prefix+"minScore"+suffix).map(_.head.toFloat).getOrElse(0.0f)
  def toJson: JsObject = Json.obj(prefix+"query"+suffix->query,prefix+"minScore"+suffix->minScore)
}