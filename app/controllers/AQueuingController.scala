package controllers

import java.io.{File, PrintWriter, StringWriter}
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.codec.digest.DigestUtils
import org.apache.lucene.search.TimeLimitingCollector
import play.api.{Configuration, Environment, Logger}
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.{InjectedController, Result}
import services.IndexMetadata

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

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
    val callId = method + ":" + index.indexName + ':' + index.indexVersion + ':' + parameters.toString + ':' + pretty
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
        cause match {
          case tlcause: TimeLimitingCollector.TimeExceededException =>
            BadRequest(s"Query timeout ${tlcause.getTimeAllowed / 1000}s exceeded. If you want this to succeed, increase the timeout parameter.")
          case _ => throw cause
        }
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