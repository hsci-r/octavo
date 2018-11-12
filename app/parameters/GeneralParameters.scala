package parameters

import java.util.concurrent.ForkJoinPool

import org.apache.lucene.search.TimeLimitingCollector
import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

import scala.collection.parallel.TaskSupport
import scala.concurrent.ExecutionContext

class GeneralParameters()(implicit request: Request[AnyContent], queryMetadata: QueryMetadata) {
  import IndexAccess.{longTaskExecutionContext, longTaskForkJoinPool, longTaskTaskSupport, numLongWorkers, numShortWorkers, shortTaskExecutionContext, shortTaskForkJoinPool, shortTaskTaskSupport}
  
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val pretty: Boolean = p.get("pretty").exists(v => v.head=="" || v.head.toBoolean)
  private val timeout = p.get("timeout").map(_.head.toLong).map(t => if (t == -1l) Long.MaxValue else t*1000l).getOrElse(30000l)
  val longRunning: Boolean = timeout>60000l
  val maxResponseSize = p.get("maxResponseSize").map(_.head.toLong).map(t => if (t == -1) Long.MaxValue else t).map(Math.min(_,GeneralParameters.maxMaxResponseSize)).getOrElse(100l)*1024l*1024l
  val numWorkers: Int = if (longRunning) numLongWorkers else numShortWorkers
  val forkJoinPool: ForkJoinPool = if (longRunning) longTaskForkJoinPool else shortTaskForkJoinPool
  val taskSupport: TaskSupport = if (longRunning) longTaskTaskSupport else shortTaskTaskSupport
  val executionContext: ExecutionContext = if (longRunning) longTaskExecutionContext else shortTaskExecutionContext
  private val baseline = TimeLimitingCollector.getGlobalCounter.get
  val tlc: ThreadLocal[TimeLimitingCollector] = new ThreadLocal[TimeLimitingCollector] {
    override def initialValue(): TimeLimitingCollector = {
      val tlc = new TimeLimitingCollector(null,TimeLimitingCollector.getGlobalCounter,timeout)
      tlc.setBaseline(baseline)
      tlc
    }
  }
  val key: Option[String] = p.get("key").map(_.head)
  val force: Boolean  = p.get("force").exists(v => v.head=="" || v.head.toBoolean)
  def toJson: JsObject = Json.obj("pretty"->pretty)
  queryMetadata.json = queryMetadata.json ++ toJson
  queryMetadata.longRunning = longRunning
  queryMetadata.key = key
}

object GeneralParameters {
  val maxMaxResponseSize = Runtime.getRuntime.maxMemory/4/1024/1024
}