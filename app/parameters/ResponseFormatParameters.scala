package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

trait ResponseFormatParametersT extends WithToJson {
  protected val request: Request[AnyContent]
  protected val ia: IndexAccess
  protected val queryMetadata: QueryMetadata

  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)

  /** desired response format */
  val responseFormat = p.get("format").map(v => ResponseFormat.withName(v.head.toUpperCase)).getOrElse(ResponseFormat.JSON)
  queryMetadata.mimeType = responseFormat.mimeType

  private val fullJson = Json.obj(
    "responseFormat"->responseFormat.entryName
  )
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.contains(pa._1)))

}

class ResponseFormatParameters()(implicit protected val request: Request[AnyContent], protected val ia: IndexAccess, protected val queryMetadata: QueryMetadata) extends ResponseFormatParametersT {}
