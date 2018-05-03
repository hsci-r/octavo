package controllers

import java.io.{File, PrintWriter, StringWriter}
import java.util.concurrent.ConcurrentHashMap

import javax.inject.{Inject,Singleton}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.lucene.search.TimeLimitingCollector
import parameters.QueryMetadata
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import play.api.{Configuration, Environment, Logger}
import services.IndexMetadata

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

@Singleton
class QueryCache @Inject() (env: Environment, configuration: Configuration) {
  val runningQueries = new ConcurrentHashMap[String,(QueryMetadata,Future[Result])]

  final val version = configuration.get[String]("app.version")

  private val mtmpDir = {
    val mtmpDir = env.getFile("tmp")
    for (i <- ('0' to '9') ++ ('a' to 'f');j <- ('0' to '9') ++ ('a' to 'f')) {
      val tmpDir = new File(mtmpDir.getPath + '/'+i+'/'+j)
      tmpDir.mkdirs()
      for (tf <- tmpDir.listFiles()) // clean up calls that were aborted when the application shut down/crashed
        if (tf.isFile && tf.getName.startsWith("result-") && tf.getName.endsWith(".json") && tf.length == 0) {
          Logger.warn("Cleaning up aborted call "+tf)
          tf.delete()
        }
    }
    mtmpDir.getPath
  }

  def files(key: String): (File,File) = (new File(tmpDir(key) + "/result-"+key+".parameters"), new File(tmpDir(key) + "/result-"+key+".json"))

  private def tmpDir(key: String): String = mtmpDir + '/' + key.charAt(0) + '/' + key.charAt(1)

}

abstract class AQueuingController(qc: QueryCache) extends InjectedController {

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  private def writeFile(file: File, content: String) {
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
  }

  protected def getOrCreateResult(method: String, index: IndexMetadata, parameters: QueryMetadata, force: Boolean, pretty: Boolean, estimate: () => Unit, call: () => JsValue)(implicit request: Request[AnyContent]): Result = {
    val callId = method + ":" + index.indexName + ':' + index.indexVersion + ':' + parameters.toString
    Logger.info(callId)
    val name = DigestUtils.sha256Hex(callId)
    if (parameters.longRunning && !parameters.key.contains(name)) {
      estimate()
      Ok("<html><body><h1>Are you sure you want to do this? Our estimate is that you'll process some "+parameters.estimatedDocumentsToProcess+" documents and can get for example "+parameters.estimatedNumberOfResults+" results</h1>If you do wish to continue, add <pre>key="+name+"</pre> to your parameters.</pre> While running, the query status can be queried from <a href=\"../status/"+name+"\">here</a>.</body></html>").as(HTML).withHeaders("X-Octavo-Key" -> name)
    } else {
      val (tf,tf2) = qc.files(name)
      if (force) tf.delete()
      if (tf.createNewFile()) {
        writeFile(tf2, callId)
        val promise = Promise[Result]
        qc.runningQueries.put(name, (parameters, promise.future))
        val startTime = System.currentTimeMillis
        try {
          estimate()
          val resultsJson = call()
          val json = Json.obj("queryMetadata" -> Json.obj("method" -> method, "parameters" -> parameters.json, "index" -> Json.obj("name" -> index.indexName, "version" -> index.indexVersion), "octavoVersion" -> qc.version, "timeTakenMS" -> (System.currentTimeMillis() - startTime)), "results" -> resultsJson)
          val jsString = if (pretty)
            Json.prettyPrint(json)
          else
            json.toString
          writeFile(tf, jsString)
          promise success Ok(jsString).as(JSON)
          qc.runningQueries.remove(name)
        } catch {
          case cause: Throwable =>
            Logger.error("Error processing " + callId + ": " + getStackTraceAsString(cause))
            tf.delete()
            qc.runningQueries.remove(name)
            cause match {
              case tlcause: TimeLimitingCollector.TimeExceededException =>
                promise success BadRequest(s"Query timeout ${tlcause.getTimeAllowed / 1000}s exceeded. If you want this to succeed, increase the timeout parameter.")
              case _ =>
                promise failure cause
                throw cause
            }
        }
      } else {
        if (qc.runningQueries.containsKey(name)) Logger.info("Waiting for result from prior call for " + callId)
        else Logger.info("Reusing ready result for " + callId)
      }
      if (!tf.exists()) InternalServerError("\"An error has occurred, please try again.\"")
      else Option(qc.runningQueries.get(name)).map(p => Await.result(p._2, Duration.Inf)).getOrElse({
        import scala.concurrent.ExecutionContext.Implicits.global
        Ok.sendFile(tf).as(JSON)
      })
    }
  }

}