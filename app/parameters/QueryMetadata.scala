package parameters

import play.api.libs.json.{JsObject, Json}

class QueryMetadata(var json: JsObject = Json.obj()) {
  override def toString: String = json.toString
}
