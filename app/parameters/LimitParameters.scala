package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class LimitParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  /** how many results to return at maximum */
  val limit: Int = p.get(prefix+"limit"+suffix).map(_.head.toInt).getOrElse(20)
  private val fullJson = Json.obj(
    prefix+"limit"+suffix->limit
  )
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.contains(pa._1)))
}
