import javax.inject.Inject
import akka.stream.Materializer
import play.api.mvc.{Result, RequestHeader, Filter}
import play.api.Logger
import play.api.routing.Router.Tags
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext

class LoggingFilter @Inject() (override implicit val mat: Materializer) extends Filter {
  def apply(nextFilter: RequestHeader => Future[Result])
           (requestHeader: RequestHeader): Future[Result] = {

    val startTime = System.currentTimeMillis
    val action = if (requestHeader.tags.contains(Tags.RouteActionMethod)) 
      requestHeader.tags(Tags.RouteController) + "." + requestHeader.tags(Tags.RouteActionMethod)
      else requestHeader.path
    
    Logger.info(f"${requestHeader.remoteAddress}%s requesting ${action}%s.")

    nextFilter(requestHeader).map { result =>
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime

      Logger.info(f"${action}%s for ${requestHeader.remoteAddress}%s took ${requestTime}%,dms and returned ${result.header.status}%s")

      result.withHeaders("Request-Time" -> requestTime.toString)
    }
  }
}