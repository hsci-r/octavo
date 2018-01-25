package controllers

import javax.inject.{Inject, Singleton}

import groovy.lang.GroovyShell
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ImportCustomizer
import play.api.libs.json.JsValue
import play.api.mvc.InjectedController
import services.IndexAccessProvider

@Singleton
class RunScriptController @Inject() (iap: IndexAccessProvider) extends InjectedController {

  def runScript(index: String) = Action { request =>
    implicit val ia = iap(index)
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val script = p.get("script").map(_.head).map(script => new GroovyShell(RunScriptController.compilerConfiguration).parse(script)).get
    script.getBinding.setProperty("ia",ia)
    Ok(script.run().asInstanceOf[JsValue]).as(JSON)
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
