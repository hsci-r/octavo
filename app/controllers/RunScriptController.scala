package controllers

import javax.inject.{Inject, Singleton}
import javax.script.Bindings
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ImportCustomizer
import play.api.http.MimeTypes
import play.api.libs.json.{JsArray, JsString, JsValue}
import play.api.mvc.InjectedController
import services.IndexAccessProvider

import scala.collection.JavaConverters._
import scala.collection.mutable

@Singleton
class RunScriptController @Inject() (iap: IndexAccessProvider) extends InjectedController {

  class BindingsInfo(var timeout: Long, val bindings: Bindings)

  val bindingInfos = new mutable.HashMap[String,BindingsInfo]()

  val cleaner = new Thread(() => {
    Thread.sleep(60000)
    val ctime = System.currentTimeMillis
    for (key <- bindingInfos.keySet)
      if (bindingInfos(key).timeout<ctime) bindingInfos.remove(key)
  }, "script context cleaner")
  cleaner.setDaemon(true)
  cleaner.start()

  def runScript() = Action { request =>
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val script = p.get("script").map(_.head)
    import services.IndexAccess.scriptEngineManager
    if (script.isEmpty) {
      Ok(JsArray(scriptEngineManager.getEngineFactories.asScala.map(ef => JsArray(ef.getNames.asScala.map(en => JsString(en)))))).as(JSON)
    } else {
      val engine = scriptEngineManager.getEngineByName(p.get("language").map(_.head).getOrElse("Groovy"))
      val bindings = p.get("bindings").map(bid => {
        val timeout = p.get("timeout").map(t => System.currentTimeMillis+(t.head.toLong*1000)).getOrElse(60000l)
        val bi = bindingInfos.get(bid.head)
        if (bi.isDefined) {
          val rbi = bi.get
          rbi.timeout = timeout
          rbi.bindings
        } else {
          val b = engine.createBindings
          bindingInfos.put(bid.head, new BindingsInfo(timeout,b))
          b
        }
      }).getOrElse(engine.createBindings)
      val contentType = p.get("contentType").map(_.head).getOrElse(MimeTypes.TEXT)
      bindings.put("iap",iap)
      try {
        val result = engine.eval(script.get, bindings)
        result match {
          case null => Ok("")
          case r: JsValue => Ok(r).as(JSON)
          case any => Ok(any.toString).as(contentType)
        }
      } catch {
        case e: Exception =>
          BadRequest(e.getClass+": "+e.getMessage()+"\n"+e.getStackTrace.mkString("\n"))
      }
    }
  } 
}

object RunScriptController {
  val compilerConfiguration = {
    val ic = new ImportCustomizer()
    ic.addStarImports("org.apache.lucene.search")
    ic.addStarImports("play.api.libs.json")
    ic.addStarImports("services")
    val c = new CompilerConfiguration()
    c.addCompilationCustomizers(ic)
    c
  }
}
