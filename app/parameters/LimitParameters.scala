package parameters

import play.api.libs.json.Json
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class LimitParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  /** how many results to return at maximum */
  val limit: Int = p.get(prefix+"limit"+suffix).map(_.head.toInt).getOrElse(20)
  private val myJson = Json.obj(
    prefix+"limit"+suffix->limit
  )
  def toJson =  myJson
  queryMetadata.json = queryMetadata.json ++ myJson
}
