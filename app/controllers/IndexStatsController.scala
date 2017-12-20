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
class IndexStatsController @Inject()(iap: IndexAccessProvider, env: Environment, conf: Configuration) extends AQueuingController(env, conf) {
  
  var dft = new mutable.HashMap[(String,String),TDigest]
  var ttft = new mutable.HashMap[(String,String),TDigest]
  
  private def calc(level: String, field: String)(implicit ia: IndexAccess): Unit = {
    synchronized {
      import IndexAccess._
      if (!dft.contains((level,field))) {
        val ir = ia.reader(level)
        val dft = TDigest.createDigest(100)
        val ttft = TDigest.createDigest(100)
        val f1 = Future {
          for (lr <- ir.leaves.asScala; (_,df) <- lr.reader.terms(field).asBytesRefAndDocFreqIterable) dft.add(df)
        }
        val f2 = Future {
          for (lr <- ir.leaves.asScala; (_,ttf) <- lr.reader.terms(field).asBytesRefAndTotalTermFreqIterable) ttft.add(ttf)
        }
        Await.ready(f1, Duration.Inf)
        Await.ready(f2, Duration.Inf)
        this.dft.put((level,field),dft)
        this.ttft.put((level,field),ttft)
      }
    }
  }
  
  def indexStats(index: String, fieldO: Option[String], from: Double, to: Double, byS: String, levelO: Option[String]) = Action { implicit request =>
    implicit val ia = iap(index)
    import IndexAccess._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val field: String = fieldO.getOrElse(ia.indexMetadata.contentField)
    val gp = GeneralParameters()
    val level = levelO.getOrElse(ia.indexMetadata.defaultLevel.id)
    val gatherFreqsPerTerm = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    val qm = Json.obj("from"->from,"to"->to,"by"->byS,"level"->level,"field"->field,"termFreqs"->gatherFreqsPerTerm) ++ gp.toJson
    val by = byS.toDouble
    getOrCreateResult("indexStats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      if (!dft.contains((level,field))) calc(level,field)
      val formatString = "%."+(byS.length-2)+"f"
      val ret = Json.obj(
        "totalDocs" -> ia.reader(level).getDocCount(field),
        "totalTerms" -> ia.reader(level).leaves.get(0).reader().terms(field).size(),
        "sumDocFreq" -> ia.reader(level).getSumDocFreq(field),
        "sumTotalTermFreq" -> ia.reader(level).getSumTotalTermFreq(field),
        "termFreqQuantiles"-> (from to to by by).map(q => Json.obj("quantile"->(formatString format q), "freq" -> ttft((level,field)).quantile(Math.min(q, 1.0)).toLong)),
        "docFreqQuantiles" -> (from to to by by).map(q => Json.obj("quantile"->(formatString format q), "freq" -> dft((level,field)).quantile(Math.min(q, 1.0)).toLong)))
      if (!gatherFreqsPerTerm) ret else
        ret ++ Json.obj("termFreqs"->ia.reader(level).leaves.get(0).reader.terms(field).asBytesRefAndDocFreqAndTotalTermFreqIterable.map(t => t._1.utf8ToString()->Json.obj("docFreq"->t._2,"totalTermFreq"->t._3)))
    })
  }
}