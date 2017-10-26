package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}

case class SamplingParameters()(implicit request: Request[AnyContent]) {
  
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  /** maximum number of documents to process */
  val maxDocs: Int = p.get("maxDocs").map(_.head.toInt).getOrElse(50000)
  def toJson: JsObject = Json.obj("maxDocs"->maxDocs)
}