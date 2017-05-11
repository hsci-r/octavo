package controllers

import javax.inject.Inject
import javax.inject.Singleton
import services.IndexAccess
import play.api.mvc.Controller
import play.api.libs.json.Json
import scala.collection.mutable.HashMap
import play.api.libs.json.JsValue
import scala.collection.mutable.ArrayBuffer
import play.api.mvc.Action
import akka.stream.Materializer
import play.api.Environment
import scala.collection.JavaConverters._
import org.apache.lucene.index.DocValues
import parameters.GeneralParameters
import services.IndexAccessProvider
import play.api.Configuration

@Singleton
class DumpController @Inject() (iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  import IndexAccess.longTaskExecutionContext
  
  def dump(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._    
    if (reader(ia.indexMetadata.levels(0).id).hasDeletions()) throw new UnsupportedOperationException("Index should not have deletions!")
    val gp = GeneralParameters()
    implicit val iec = longTaskExecutionContext
    val qm = Json.obj("method"->"dump") ++ gp.toJson
    getOrCreateResult(ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      val sdvfields = Seq("collectionID","documentID","ESTCID","language","module")
      val ndvfields = Seq("pubDateStart","pubDateEnd","documentLength","totalPages","totalParagraphs")
      val sfields = Seq("fullTitle")
      val sfieldsS = new java.util.HashSet[String]
      sfields.foreach(sfieldsS.add(_))
      val tvfields = Seq("containsGraphicOfType")
      val output = new ArrayBuffer[JsValue]
      for (lrc<-reader(ia.indexMetadata.levels(0).id).leaves().asScala;lr = lrc.reader) {
        val sdvs = sdvfields.map(p => (p,DocValues.getSorted(lr,p)))
        val ndvs = ndvfields.map(p => (p,DocValues.getNumeric(lr,p)))
        for (i <- 0 until lr.maxDoc) {
          val values = new HashMap[String,JsValue]
          for ((f,dv) <- sdvs) values += (f -> Json.toJson(dv.get(i).utf8ToString))
          for ((f,dv) <- ndvs) values += (f -> Json.toJson(dv.get(i)))
          val d = lr.document(i,sfieldsS)
          for (f <- sfields) values += (f -> Json.toJson(d.get(f)))
          for (f <- tvfields; tv = lr.getTermVector(i, f); if tv != null) {
            val fte = tv.iterator
            val vals = new HashMap[String,JsValue]
            var br = fte.next()
            while (br!=null) {
              vals += (br.utf8ToString -> Json.toJson(fte.docFreq))
              br = fte.next()
            }
            values += (f -> Json.toJson(vals))
          }
          output += Json.toJson(values)
        }
      }
      Json.toJson(output)
    })
  }
  
}
