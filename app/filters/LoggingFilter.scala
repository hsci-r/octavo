import javax.inject.Inject
import akka.stream.Materializer
import play.api.mvc.{Result, RequestHeader, Filter}
import play.api.Logger
import scala.concurrent.Future
import com.google.common.net.InetAddresses
import play.api.routing.HandlerDef
import play.api.routing.Router
import scala.concurrent.ExecutionContext

class LoggingFilter @Inject() (implicit val mat: Materializer, ec: ExecutionContext) extends Filter {
  def apply(nextFilter: RequestHeader => Future[Result])
           (requestHeader: RequestHeader): Future[Result] = {

    val startTime = System.currentTimeMillis
    val action = if (requestHeader.attrs.contains(Router.Attrs.HandlerDef)) { 
      val handlerDef = requestHeader.attrs(Router.Attrs.HandlerDef)
      handlerDef.controller + "." + handlerDef.method
    } else requestHeader.path
    
    val remoteHost = InetAddresses.forString(requestHeader.remoteAddress).getCanonicalHostName
      
    Logger.info(f"${remoteHost}%s requesting ${action}%s.")

    nextFilter(requestHeader).transform(result => { 
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime

      Logger.info(f"${action}%s for ${remoteHost}%s took ${requestTime}%,dms and returned ${result.header.status}%s")

      result.withHeaders("Request-Time" -> requestTime.toString)
    }, failure => {
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime
      Logger.warn(f"${action}%s for ${remoteHost}%s took ${requestTime}%,dms, but failed.", failure)
      failure
    })
  }
}