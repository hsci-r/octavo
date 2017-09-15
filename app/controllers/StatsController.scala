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
import play.api.mvc.InjectedController
import scala.collection.mutable.HashMap
import play.api.Environment
import play.api.Configuration
import parameters.GeneralParameters

@Singleton
class StatsController @Inject() (iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  var dft = new HashMap[String,TDigest]
  var ttft = new HashMap[String,TDigest]
  
  private def calc(level: String)(implicit ia: IndexAccess): Unit = {
    synchronized {
      import IndexAccess._
      if (!dft.contains(level)) {
        val ir = ia.reader(level)
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
        this.dft.put(level,dft)
        this.ttft.put(level,ttft)
      }
    }
  }
  
  def stats(index: String, from: Double, to: Double, byS: String, levelO: Option[String]) = Action { implicit request =>
    implicit val ia = iap(index)
    import ia._
    val gp = GeneralParameters()
    val level = levelO.getOrElse(ia.indexMetadata.defaultLevel.id)
    val qm = Json.obj("from"->from,"to"->to,"by"->byS,"level"->level)
    val by = byS.toDouble
    getOrCreateResult("stats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      if (!dft.contains(level)) calc(level)
      val formatString = "%."+(byS.length-2)+"f"
      Json.obj(
        "totalDocs" -> ia.reader(level).getDocCount(ia.indexMetadata.contentField),
        "totalTerms" -> ia.reader(level).leaves.get(0).reader().terms(ia.indexMetadata.contentField).size(),
        "totalDocFreq" -> ia.reader(level).getSumDocFreq(ia.indexMetadata.contentField),
        "totalTermFreq" -> ia.reader(level).getSumTotalTermFreq(ia.indexMetadata.contentField),
        "termFreqQuantiles"-> (from to to by by).map(q => Json.obj("quantile"->(formatString format q), "freq" -> ttft(level).quantile(Math.min(q, 1.0)).toLong)),
        "docFreqQuantiles" -> (from to to by by).map(q => Json.obj("quantile"->(formatString format q), "freq" -> dft(level).quantile(Math.min(q, 1.0)).toLong)))
      })
  }
}