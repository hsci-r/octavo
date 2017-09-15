package parameters

import play.api.mvc.Request
import scala.concurrent.ExecutionContext
import org.apache.lucene.search.TimeLimitingCollector
import play.api.mvc.AnyContent
import services.IndexAccess
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import scala.collection.immutable.Map
import play.api.libs.json.JsObject
import scala.collection.parallel.TaskSupport
import java.util.concurrent.ForkJoinPool

case class SamplingParameters()(implicit request: Request[AnyContent]) {
  
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  /** maximum number of documents to process */
  val maxDocs: Int = p.get("maxDocs").map(_(0).toInt).getOrElse(50000)
  def toJson(): JsObject = Json.obj("maxDocs"->maxDocs)
}