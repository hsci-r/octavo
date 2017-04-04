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

@Singleton
class StatsController @Inject() (ia: IndexAccess) extends Controller {
  import ia._
  import IndexAccess._
  
  var dft: TDigest = null
  var ttft: TDigest = null
  
  def calc(): Unit = {
    synchronized {
      if (dft == null) {
        val ir = ia.reader(ia.indexMetadata.defaultLevel.id)
        val dft = TDigest.createDigest(100)
        val ttft = TDigest.createDigest(100)
        val f1 = Future {
          for (lr <- ir.leaves.asScala; (term,df) <- lr.reader.terms(indexMetadata.contentField).asBytesRefAndDocFreqIterable()) dft.add(df)
        }
        val f2 = Future {
          for (lr <- ir.leaves.asScala; (term,ttf) <- lr.reader.terms(indexMetadata.contentField).asBytesRefAndTotalTermFreqIterable()) ttft.add(ttf)
        }
        Await.ready(f1, Duration.Inf)
        Await.ready(f2, Duration.Inf)
        this.dft = dft
        this.ttft = ttft
      }
    }
  }
  
  def stats(from: Double, to: Double, by: Double) = Action {
    if (dft == null) calc()
    Ok(Json.prettyPrint(Json.obj(
        "from"->from,
        "to"->to,
        "by"->by,
        "totalDocs" -> ia.reader(ia.indexMetadata.defaultLevel.id).getDocCount(indexMetadata.contentField),
        "totalTerms" -> ia.reader(ia.indexMetadata.defaultLevel.id).leaves.get(0).reader().terms(indexMetadata.contentField).size(),
        "totalDocFreq" -> ia.reader(ia.indexMetadata.defaultLevel.id).getSumDocFreq(indexMetadata.contentField),
        "totalTermFreq" -> ia.reader(ia.indexMetadata.defaultLevel.id).getSumTotalTermFreq(indexMetadata.contentField),
        "termFreqQuantiles"-> (from to to by by).map(q => Json.obj(""+q -> ttft.quantile(q).toLong)),
        "docFreqQuantiles" -> (from to to by by).map(q => Json.obj(""+q -> dft.quantile(q).toLong)))))
  }  
}