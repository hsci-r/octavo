package controllers

import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import play.api.libs.json.JsValue
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.search.Scorer
import org.apache.lucene.index.LeafReaderContext
import play.api.libs.json.Json
import javax.inject.Inject
import org.apache.lucene.queryparser.classic.QueryParser
import javax.inject.Named
import services.IndexAccess
import parameters.SumScaling
import play.api.libs.json.JsObject
import scala.collection.JavaConverters._
import com.tdunning.math.stats.TDigest
import org.apache.commons.lang3.SerializationUtils
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import services.IndexAccessProvider
import groovy.lang.GroovyShell
import org.codehaus.groovy.control.customizers.ImportCustomizer
import org.codehaus.groovy.control.CompilerConfiguration
import play.api.mvc.InjectedController

@Singleton
class RunScriptController @Inject() (iap: IndexAccessProvider) extends InjectedController {
  
  private val compilerConfiguration = {
    val ic = new ImportCustomizer()
    ic.addStarImports("org.apache.lucene.search")
    ic.addStarImports("play.api.libs.json")
    ic.addStaticStars("services.TermVectors")
    val c = new CompilerConfiguration()
    c.addCompilationCustomizers(ic)
    c
  }
  
  def runScript(index: String) = Action { request =>
    implicit val ia = iap(index)
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val script = p.get("script").map(_(0)).map(script => new GroovyShell(compilerConfiguration).parse(script)).get
    script.getBinding.setProperty("ia",ia)
    Ok(script.run().asInstanceOf[JsValue]).as(JSON)
  } 
}