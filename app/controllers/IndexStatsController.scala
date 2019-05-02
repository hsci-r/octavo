package controllers

import com.tdunning.math.stats.TDigest
import javax.inject.{Inject, Singleton}
import org.apache.lucene.document.{DoublePoint, FloatPoint, LongPoint}
import org.apache.lucene.geo.GeoEncodingUtils
import org.apache.lucene.search.DocIdSetIterator
import parameters.{GeneralParameters, QueryMetadata}
import play.api.libs.json.Json
import services.{IndexAccess, IndexAccessProvider, IndexedFieldType}

import scala.collection.JavaConverters._
import scala.collection.mutable

@Singleton
class IndexStatsController @Inject()(iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  val dft = new mutable.HashMap[(String,String),TDigest]
  val ttft = new mutable.HashMap[(String,String),TDigest]

  private def calcFrequencyQuantiles(level: String, field: String)(implicit ia: IndexAccess): Unit = {
    synchronized {
      import IndexAccess._
      if (!dft.contains((level,field))) {
        val ir = ia.reader(level)
        val dft = TDigest.createDigest(100)
        val ttft = TDigest.createDigest(100)
        for (lr <- ir.leaves.asScala; (_,df,ttf) <- lr.reader.terms(field).asBytesRefAndDocFreqAndTotalTermFreqIterable) {
          dft.add(df)
          ttft.add(ttf)
        }
        this.dft.put((level,field),dft)
        this.ttft.put((level,field),ttft)
      }
    }
  }

  val histograms = new mutable.HashMap[(String,String),TDigest]

  private def calcNumericHistogram(level: String, field: String)(implicit ia: IndexAccess): Unit = {
    val lr = ia.reader(level).leaves.get(0).reader
    val pv = lr.getPointValues(field)
    ia.indexMetadata.levelMap(level).fields(field).indexedAs match {
      case IndexedFieldType.INTPOINT | IndexedFieldType.LONGPOINT =>
        val dv = lr.getNumericDocValues(field)
        val histogram = TDigest.createDigest(100)
        while (dv.nextDoc()!=DocIdSetIterator.NO_MORE_DOCS)
          histogram.add(dv.longValue)
        histograms.put((level,field),histogram)
      case IndexedFieldType.LONGPOINT => Json.obj("min" -> LongPoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> LongPoint.decodeDimension(pv.getMaxPackedValue, 0))
      case IndexedFieldType.FLOATPOINT => Json.obj("min" -> FloatPoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> FloatPoint.decodeDimension(pv.getMaxPackedValue, 0))
      case IndexedFieldType.DOUBLEPOINT => Json.obj("min" -> DoublePoint.decodeDimension(pv.getMinPackedValue, 0), "max" -> DoublePoint.decodeDimension(pv.getMaxPackedValue, 0))
      case IndexedFieldType.LATLONPOINT => Json.obj("min" -> Json.obj("lat" -> GeoEncodingUtils.decodeLatitude(pv.getMinPackedValue, 0), "lon" -> GeoEncodingUtils.decodeLongitude(pv.getMinPackedValue, Integer.BYTES)), "max" -> Json.obj("lat" -> GeoEncodingUtils.decodeLatitude(pv.getMaxPackedValue, 0), "lon" -> GeoEncodingUtils.decodeLongitude(pv.getMaxPackedValue, Integer.BYTES)))
    }
  }

  def indexStats(index: String, fieldO: Option[String], from: BigDecimal, to: BigDecimal, byS: String, levelO: Option[String]) = Action { implicit request =>
    implicit val ia = iap(index)
    import IndexAccess._
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val field: String = fieldO.getOrElse(ia.indexMetadata.contentField)
    val level = levelO.getOrElse(ia.indexMetadata.defaultLevel.id)
    val gatherFreqsPerTerm = p.get("termFreqs").exists(v => v.head=="" || v.head.toBoolean)
    val quantiles = p.get("quantiles").exists(v => v.head=="" || v.head.toBoolean)
    val bins = p.get("bins").map(_.head.toInt).getOrElse(0)
    val binWidth = p.get("binWidth").map(_.head.toInt).getOrElse(0)
    val by = BigDecimal(byS)
    implicit val qm = new QueryMetadata(Json.obj(
      "level"->level,
      "field"->field,
      "termFreqs"->gatherFreqsPerTerm,
      "quantiles"->quantiles,
      "from"->from,
      "to"->to,
      "by"->byS,
      "bins"->bins,
      "binWidth"->binWidth
    ))
    val gp = new GeneralParameters()
    qm.longRunning = false
    getOrCreateResult("indexStats", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      // will not be called because longRunning = false
    }, () => {
      val formatString = "%."+(byS.length-2)+"f"
      var ret = Json.obj(
        "totalDocs" -> ia.reader(level).getDocCount(field)/*,
        "totalTerms" -> ia.reader(level).leaves.get(0).reader().terms(field).size(),
        "sumDocFreq" -> ia.reader(level).getSumDocFreq(field),
        "sumTotalTermFreq" -> ia.reader(level).getSumTotalTermFreq(field)*/)
      if (bins!=0) {
        if (!histograms.contains((level,field))) calcNumericHistogram(level,field)
        ret = ret ++ Json.obj(
          "quantiles"->(from to to by by).map(q => Json.obj("quantile" -> (formatString format q), "freq" -> histograms((level, field)).quantile(Math.min(q.doubleValue, 1.0)).toLong))
        )
      }
      if (quantiles) {
        if (!dft.contains((level,field))) calcFrequencyQuantiles(level,field)
        ret = ret ++ Json.obj(
          "termFreqQuantiles" -> (from to to by by).map(q => Json.obj("quantile" -> (formatString format q), "freq" -> ttft((level, field)).quantile(Math.min(q.doubleValue, 1.0)).toLong)),
          "docFreqQuantiles" -> (from to to by by).map(q => Json.obj("quantile" -> (formatString format q), "freq" -> dft((level, field)).quantile(Math.min(q.doubleValue, 1.0)).toLong))
        )
      }
      if (gatherFreqsPerTerm)
        ret = ret ++ Json.obj("termFreqs"->ia.reader(level).leaves.get(0).reader.terms(field).asBytesRefAndDocFreqAndTotalTermFreqIterable.map(t => t._1.utf8ToString()->Json.obj("docFreq"->t._2,"totalTermFreq"->t._3)))
      Left(ret)
    })
  }
}