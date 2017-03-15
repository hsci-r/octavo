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
import play.api.mvc.Action
import javax.inject.Inject
import org.apache.lucene.queryparser.classic.QueryParser
import play.api.mvc.Controller
import javax.inject.Named
import services.IndexAccess
import parameters.SumScaling
import play.api.libs.json.JsObject
import scala.collection.JavaConverters._

@Singleton
class StatsController @Inject() (ia: IndexAccess) extends Controller {
  import ia._
  
  def stats(quantile: Int) = Action {
    val ir = ia.reader(ia.indexMetadata.levels(0).id)
    for (lr <- ir.leaves.asScala; term <- lr.reader.terms("content")) {
    }
    Ok(Json.prettyPrint(Json.obj(
        "quantile"->quantile,
        "termFreqQuantiles"-> "",
        "docFreqQuantiles" -> "")))
  }  
}