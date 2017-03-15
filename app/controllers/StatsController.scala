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
import com.tdunning.math.stats.TDigest
import org.apache.commons.lang3.SerializationUtils
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream

@Singleton
class StatsController @Inject() (ia: IndexAccess) extends Controller {
  import ia._
  import IndexAccess._
  
  var dft: TDigest = null
  var ttft: TDigest = null
  
  def calc(): Unit = {
    val ir = ia.reader(ia.indexMetadata.levels(0).id)
    dft = TDigest.createDigest(100)
    ttft = TDigest.createDigest(100)
    for (lr <- ir.leaves.asScala; (term,df,ttf) <- lr.reader.terms("content").asBytesRefAndDocFreqAndTotalTermFreqIterable()) {
      dft.add(df)
      ttft.add(ttf)
    }
  }
  
  def stats(quantile: Int, from: Int, toO: Option[Int], by: Int) = Action {
    if (dft == null) calc()
    val tq = quantile.toDouble
    val to = toO.getOrElse(quantile)
    Ok(Json.prettyPrint(Json.obj(
        "quantile"->quantile,
        "from"->from,
        "to"->to,
        "by"->by,
        "termFreqQuantiles"-> (from to to by by).map(q => Json.obj(""+q.toDouble/tq -> ttft.quantile(q.toDouble/tq).toInt)),
        "docFreqQuantiles" -> (from to to by by).map(q => Json.obj(""+q.toDouble/tq ->dft.quantile(q.toDouble/tq).toLong)))))
  }  
}