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
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import services.IndexAccessProvider

@Singleton
class StatsController @Inject() (iap: IndexAccessProvider) extends Controller {
  
  var dft: TDigest = null
  var ttft: TDigest = null
  
  private def calc()(implicit ia: IndexAccess): Unit = {
    synchronized {
      import IndexAccess._
      if (dft == null) {
        val ir = ia.reader(ia.indexMetadata.defaultLevel.id)
        val dft = TDigest.createDigest(100)
        val ttft = TDigest.createDigest(100)
        val f1 = Future {
          for (lr <- ir.leaves.asScala; (term,df) <- lr.reader.terms(ia.indexMetadata.contentField).asBytesRefAndDocFreqIterable()) dft.add(df)
        }
        val f2 = Future {
          for (lr <- ir.leaves.asScala; (term,ttf) <- lr.reader.terms(ia.indexMetadata.contentField).asBytesRefAndTotalTermFreqIterable()) ttft.add(ttf)
        }
        Await.ready(f1, Duration.Inf)
        Await.ready(f2, Duration.Inf)
        this.dft = dft
        this.ttft = ttft
      }
    }
  }
  
  def stats(index: String, from: Double, to: Double, byS: String) = Action {
    val by = byS.toDouble
    implicit val ia = iap(index)
    if (dft == null) calc()
    val formatString = "%."+(byS.length-2)+"f"
    Ok(Json.prettyPrint(Json.obj(
        "from"->from,
        "to"->to,
        "by"->by,
        "totalDocs" -> ia.reader(ia.indexMetadata.defaultLevel.id).getDocCount(ia.indexMetadata.contentField),
        "totalTerms" -> ia.reader(ia.indexMetadata.defaultLevel.id).leaves.get(0).reader().terms(ia.indexMetadata.contentField).size(),
        "totalDocFreq" -> ia.reader(ia.indexMetadata.defaultLevel.id).getSumDocFreq(ia.indexMetadata.contentField),
        "totalTermFreq" -> ia.reader(ia.indexMetadata.defaultLevel.id).getSumTotalTermFreq(ia.indexMetadata.contentField),
        "termFreqQuantiles"-> (from to to by by).map(q => Json.obj((formatString format q) -> ttft.quantile(Math.min(q, 1.0)).toLong)),
        "docFreqQuantiles" -> (from to to by by).map(q => Json.obj((formatString format q) -> dft.quantile(Math.min(q, 1.0)).toLong)))))
  }  
}