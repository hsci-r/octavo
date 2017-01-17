package parameters

import play.api.mvc.Request
import scala.concurrent.ExecutionContext
import org.apache.lucene.search.TimeLimitingCollector
import play.api.mvc.AnyContent
import services.IndexAccess

case class GeneralParameters(implicit request: Request[AnyContent]) {
  import IndexAccess.{longTaskExecutionContext,shortTaskExecutionContext}
  
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val pretty: Boolean = p.get("pretty").exists(v => v(0)=="" || v(0).toBoolean)
  val limit: Int = p.get("limit").map(_(0).toInt).getOrElse(20)
  val maxDocs: Int = p.get("maxDocs").map(_(0).toInt).getOrElse(50000)
  private val timeout = p.get("timeout").map(_(0).toLong).map(t => if (t == -1l) Long.MaxValue else t*1000l).getOrElse(30000l)
  val executionContext: ExecutionContext = if (timeout>60000l) longTaskExecutionContext else shortTaskExecutionContext
  private val baseline = TimeLimitingCollector.getGlobalCounter.get
  val tlc: ThreadLocal[TimeLimitingCollector] = new ThreadLocal[TimeLimitingCollector] {
    override def initialValue(): TimeLimitingCollector = {
      val tlc = new TimeLimitingCollector(null,TimeLimitingCollector.getGlobalCounter,timeout)
      tlc.setBaseline(baseline)
      tlc
    }
  }
  val force: Boolean  = p.get("force").exists(v => v(0)=="" || v(0).toBoolean)
  override def toString() = s"maxDocs:$maxDocs, pretty: $pretty, limit: $limit"
}