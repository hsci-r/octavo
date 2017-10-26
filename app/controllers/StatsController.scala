package controllers

import javax.inject.{Inject, Singleton}

import com.tdunning.math.stats.TDigest
import parameters.GeneralParameters
import play.api.{Configuration, Environment}
import play.api.libs.json.Json
import services.{IndexAccess, IndexAccessProvider}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

@Singleton
class StatsController @Inject() (iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  var dft = new mutable.HashMap[String,TDigest]
  var ttft = new mutable.HashMap[String,TDigest]
  
  private def calc(level: String)(implicit ia: IndexAccess): Unit = {
    synchronized {
      import IndexAccess._
      if (!dft.contains(level)) {
        val ir = ia.reader(level)
        val dft = TDigest.createDigest(100)
        val ttft = TDigest.createDigest(100)
        val f1 = Future {
          for (lr <- ir.leaves.asScala; (_,df) <- lr.reader.terms(ia.indexMetadata.contentField).asBytesRefAndDocFreqIterable) dft.add(df)
        }
        val f2 = Future {
          for (lr <- ir.leaves.asScala; (_,ttf) <- lr.reader.terms(ia.indexMetadata.contentField).asBytesRefAndTotalTermFreqIterable) ttft.add(ttf)
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