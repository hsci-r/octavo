package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}

class SamplingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], queryMetadata: QueryMetadata) {
  
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  /** maximum number of documents to process */
  val maxDocs: Int = p.get(prefix+"maxDocs"+suffix).map(_.head.toInt).getOrElse(50000)
  private val fullJson: JsObject = Json.obj(prefix+"maxDocs"+suffix->maxDocs)
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.contains(pa._1)))
}