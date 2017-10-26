package parameters

import java.util.concurrent.ForkJoinPool

import org.apache.lucene.search.TimeLimitingCollector
import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

import scala.collection.parallel.TaskSupport
import scala.concurrent.ExecutionContext

case class GeneralParameters()(implicit request: Request[AnyContent]) {
  import IndexAccess.{longTaskExecutionContext, longTaskForkJoinPool, longTaskTaskSupport, shortTaskExecutionContext, shortTaskForkJoinPool, shortTaskTaskSupport}
  
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val pretty: Boolean = p.get("pretty").exists(v => v.head=="" || v.head.toBoolean)
  /** maximum number of documents to process */
  val maxDocs: Int = p.get("maxDocs").map(_.head.toInt).getOrElse(50000)
  private val timeout = p.get("timeout").map(_.head.toLong).map(t => if (t == -1l) Long.MaxValue else t*1000l).getOrElse(30000l)
  val forkJoinPool: ForkJoinPool = if (timeout>60000l) longTaskForkJoinPool else shortTaskForkJoinPool
  val taskSupport: TaskSupport = if (timeout>60000l) longTaskTaskSupport else shortTaskTaskSupport
  val executionContext: ExecutionContext = if (timeout>60000l) longTaskExecutionContext else shortTaskExecutionContext
  private val baseline = TimeLimitingCollector.getGlobalCounter.get
  val tlc: ThreadLocal[TimeLimitingCollector] = new ThreadLocal[TimeLimitingCollector] {
    override def initialValue(): TimeLimitingCollector = {
      val tlc = new TimeLimitingCollector(null,TimeLimitingCollector.getGlobalCounter,timeout)
      tlc.setBaseline(baseline)
      tlc
    }
  }
  val force: Boolean  = p.get("force").exists(v => v.head=="" || v.head.toBoolean)
  def toJson: JsObject = Json.obj("maxDocs"->maxDocs,"pretty"->pretty)
}