package controllers

import java.io.{File, PrintWriter, StringWriter}
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap

import com.google.common.net.InetAddresses
import groovy.lang.GroovyRuntimeException
import javax.inject.{Inject, Singleton}
import javax.script.ScriptException
import org.apache.commons.codec.digest.DigestUtils
import org.apache.lucene.search.TimeLimitingCollector
import parameters.{GeneralParameters, QueryMetadata}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import play.api.{Configuration, Environment, Logging}
import play.twirl.api.HtmlFormat
import services.IndexMetadata

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

@Singleton
class QueryCache @Inject() (env: Environment, configuration: Configuration) extends Logging {
  val runningQueries = new ConcurrentHashMap[String,(QueryMetadata,Future[Result])]

  final val version = configuration.get[String]("app.version")

  final val cauths = configuration.getOptional[Configuration]("auths").map(c => {
    c.keys.map(k => k -> c.get[String](k)).toMap
  }).getOrElse(Map.empty)

  def checkAuth(index: IndexMetadata, request: Request[AnyContent]): Option[String] = {
    if (!cauths.contains(index.indexId) && index.auths.isEmpty) return Some("?")
    val cauth = cauths.get(index.indexId).map(s => new String(Base64.getDecoder.decode(s)))
      .map(a => a.splitAt(a.indexOf(':')))
    request.headers.getAll("Authorization")
      .filter(a => a.toLowerCase.startsWith("basic "))
      .map(_.substring(6))
      .map(s => new String(Base64.getDecoder.decode(s)))
      .map(a => a.splitAt(a.indexOf(':')))
      .find(p => cauth.contains(p) || index.auths.get(p._1).contains(p._2.drop(1)))
      .map(_._1)
  }

  private val mtmpDir = {
    val mtmpDir = env.getFile("tmp")
    for (i <- ('0' to '9') ++ ('a' to 'f');j <- ('0' to '9') ++ ('a' to 'f')) {
      val tmpDir = new File(mtmpDir.getPath + '/'+i+'/'+j)
      tmpDir.mkdirs()
      for (tf <- tmpDir.listFiles()) // clean up calls that were aborted when the application shut down/crashed
        if (tf.isFile && tf.getName.endsWith(".result") && tf.length == 0) {
          logger.warn("Cleaning up aborted call "+tf)
          tf.delete()
        }
    }
    mtmpDir.getPath
  }

  def files(key: String): (File,File) = (new File(tmpDir(key) + "/"+key+".parameters"), new File(tmpDir(key) + "/"+key+".result"))

  private def tmpDir(key: String): String = mtmpDir + '/' + key.charAt(0) + '/' + key.charAt(1)

}

class ResponseTooBigException(val limit: Long) extends RuntimeException()

abstract class AQueuingController(qc: QueryCache) extends InjectedController with Logging {

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  private def writeFile(file: File, content: String) = {
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
  }

  protected def getOrCreateResult(method: String, index: IndexMetadata, parameters: QueryMetadata, force: Boolean, pretty: Boolean, estimate: () => Unit, call: () => Either[JsValue,Result])(implicit request: Request[AnyContent]): Result = {
    val auth = qc.checkAuth(index,request)
    if (auth.isEmpty) return Unauthorized.withHeaders("WWW-Authenticate" -> """Basic realm="Restricted"""")
    val fcallId = method + ":" + index.indexName + ':' + index.indexVersion + ':' + parameters.fullJson.toString + ':' + parameters.mimeType
    val ndcallId = method + ":" + index.indexName + ':' + index.indexVersion + ':' + parameters.nonDefaultJson.toString
    val startTime = System.currentTimeMillis
    val name = DigestUtils.sha256Hex(fcallId)
    val remoteId = auth.get + "@" + InetAddresses.forString(request.headers.get("X-Forwarded-For").map(xf => {
      val i = xf.indexOf(',')
      if (i != -1) xf.substring(0,i) else xf
    }).getOrElse(request.remoteAddress)).getCanonicalHostName
    val qm = Json.obj("callId"->name, "method" -> method, "parameters" -> parameters.nonDefaultJson, "index" -> Json.obj("name" -> index.indexName, "version" -> index.indexVersion), "octavoVersion" -> qc.version, "fullParameters" -> parameters.fullJson)
    if (parameters.longRunning && !parameters.key.contains(name)) {
      try {
        estimate()
        Ok("<html><body>You are about to run the following query:<br /><pre>"+HtmlFormat.escape(Json.prettyPrint(qm))+"</pre><h1>Are you sure you want to do this? Our estimate is that you'll process some "+parameters.estimatedDocumentsToProcess+" documents and can get for example "+parameters.estimatedNumberOfResults+" results</h1>If you do wish to continue, add <pre>key="+name+"</pre> to your parameters.</pre> While running, the query status can be queried from <a href=\"../status/"+name+"\">here</a>.</body></html>").as(HTML).withHeaders("X-Octavo-Key" -> name)
      } catch {
        case cause: Throwable =>
          logger.error(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - Error processing estimate for " + ndcallId + "(" + fcallId + "): " + getStackTraceAsString(cause))
          cause match {
            case tlcause: TimeLimitingCollector.TimeExceededException =>
              BadRequest(s"Query estimate timeout ${tlcause.getTimeAllowed / 1000}s exceeded. This is probably due to a bad query, but if you want still want to continue, increase the etimeout parameter.")
            case _ =>
              throw cause
          }
      }
    } else {
      val (pf,tf) = qc.files(name)
      if (force) tf.delete()
      val future =
        if (tf.createNewFile()) {
          logger.info(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - Running call " + ndcallId + " ("+name+")")
          logger.debug(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - full parameters: "+fcallId + " ("+name+")")
          writeFile(pf, Json.prettyPrint(qm))
          val promise = Promise[Result]
          qc.runningQueries.put(name, (parameters, promise.future))
          val startTime = System.currentTimeMillis
          try {
            estimate()
            call() match {
              case Left(resultsJson) =>
                val json = Json.obj("queryMetadata" -> (qm ++ Json.obj("timeTakenMS" -> (System.currentTimeMillis() - startTime))), "result" -> resultsJson)
                val jsString = if (pretty)
                  Json.prettyPrint(json)
                else
                  json.toString
                writeFile(tf, jsString)
                promise success Ok(jsString).as(JSON)
              case Right(result) =>
                tf.delete()
                promise success result
            }
            qc.runningQueries.remove(name)
          } catch {
            case cause: Throwable =>
              logger.error(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - Error processing " + ndcallId + " ("+fcallId+"): " + getStackTraceAsString(cause))
              tf.delete()
              qc.runningQueries.remove(name)
              cause match {
                case tlcause: TimeLimitingCollector.TimeExceededException =>
                  promise success BadRequest(s"Query timeout ${tlcause.getTimeAllowed / 1000}s exceeded. If you want this to succeed, increase the timeout parameter.")
                case ccause: ScriptException =>
                  promise success BadRequest("Error in script: "+ccause.getMessage)
                case ccause: GroovyRuntimeException =>
                  promise success BadRequest("Error in script: "+ccause.getMessage)
                case ccause: ResponseTooBigException =>
                  promise success BadRequest(s"Maximum response size estimate (${ccause.limit/1024/1024}MB) exceeded. If you want this to succeed, you may try increasing the maxResponseSize parameter, up to ${GeneralParameters.maxMaxResponseSize} (MB).")
                case ccause: IllegalArgumentException =>
                  promise success BadRequest(s"Error in request: ${ccause.getMessage}")
                case _ =>
                  promise success InternalServerError(s"Error processing request: ${getStackTraceAsString(cause)}")
/*                case _ =>
                  promise failure cause
                  throw cause */
              }
          }
          promise.future
        } else
          Option(qc.runningQueries.get(name)) match {
            case Some(f) =>
              logger.info(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - Waiting for result from prior call for " + ndcallId + " ("+name+")")
              logger.debug(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - full parameters: "+fcallId + " ("+name+")")
              f._2
            case None =>
              import scala.concurrent.ExecutionContext.Implicits.global
              if (tf.exists()) {
                logger.info(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - Reusing result from prior call for " + ndcallId + " ("+name+")")
                logger.debug(remoteId + " % [" + name.substring(0,6).toUpperCase + "] - full parameters: "+fcallId + " ("+name+")")
                Future(Ok.sendFile(tf).as(parameters.mimeType))
              } else Future(InternalServerError("\"An error has occurred, please try again.\""))
          }
      val result = Await.result(future, Duration.Inf)
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime
      logger.info(f"$remoteId%s %% [${name.substring(0,6).toUpperCase}] - After $requestTime%,dms, returning ${result.body.contentLength.getOrElse(-1L)}%,d bytes in ${parameters.mimeType}%s with status ${result.header.status}%s for call $ndcallId%s ($name%s).")
      result
    }
  }

}