package controllers

import play.api.mvc.Result
import scala.concurrent.ExecutionContext
import java.io.File
import play.api.Logger
import org.apache.lucene.search.TimeLimitingCollector
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import java.util.concurrent.ConcurrentHashMap
import java.io.PrintWriter
import scala.concurrent.Future
import javax.inject.Inject
import akka.stream.Materializer
import java.io.StringWriter
import play.api.Environment
import play.api.libs.json.JsValue
import play.api.libs.json.JsObject
import play.api.libs.json.Json
import play.api.Configuration
import services.IndexMetadata
import play.api.mvc.InjectedController
import org.apache.commons.codec.digest.DigestUtils

abstract class AQueuingController(env: Environment, configuration: Configuration) extends InjectedController {
  
  private final val version = configuration.get[String]("app.version") 
  
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
  
  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  
  val processing = new ConcurrentHashMap[String,Future[Result]]
  
  private def writeFile(file: File, content: String) {
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
  }

  protected def getOrCreateResult(method: String, index: IndexMetadata, parameters: JsObject, force: Boolean, pretty: Boolean, call: () => JsValue)(implicit ec: ExecutionContext): Result = {
    val callId = method + ":" + index.indexName + ':' + index.indexVersion + ':' + parameters.toString
    Logger.info(callId)
    val name = DigestUtils.sha256Hex(callId)
    val tmpDir = mtmpDir+'/'+name.charAt(0)+'/'+name.charAt(1)
    val tf = new File(tmpDir+"/result-"+name+".json")
    if (force) tf.delete()
    if (tf.createNewFile()) {
      val tf2 = new File(tmpDir+"/result-"+name+".parameters")
      writeFile(tf2, callId)
      val future = Future {
        val startTime = System.currentTimeMillis
        val resultsJson = call() 
        val json = Json.obj("queryMetadata"->Json.obj("method"->method,"parameters"->parameters,"index"->Json.obj("name"->index.indexName,"version"->index.indexVersion),"octavoVersion"->version,"timeTakenMS"->(System.currentTimeMillis()-startTime)),"results"->resultsJson)
        if (pretty)
          Json.prettyPrint(json)
        else
          json.toString
      }.map(content => {
        writeFile(tf, content)
        processing.remove(name)
        Ok(content).as(JSON)
      }).recover{ case cause =>
        Logger.error("Error processing "+callId+": "+getStackTraceAsString(cause))
        tf.delete()
        processing.remove(name)
        if (cause.isInstanceOf[TimeLimitingCollector.TimeExceededException]) {
          val tlcause = cause.asInstanceOf[TimeLimitingCollector.TimeExceededException]
          BadRequest(s"Query timeout ${tlcause.getTimeAllowed/1000}s exceeded. If you want this to succeed, increase the timeout parameter.")
        } else throw cause
      }
      processing.put(name, future)
    } else {
      if (processing.containsKey(name)) Logger.info("Waiting for result from prior call for "+callId) 
      else Logger.info("Reusing ready result for "+callId)
    }
    val f = new File(tmpDir+"/result-"+name+".json")
    if (!f.exists()) InternalServerError("\"An error has occurred, please try again.\"")
    else Option(processing.get(name)).map(Await.result(_, Duration.Inf)).getOrElse(Ok.sendFile(f).as(JSON))
  }

}