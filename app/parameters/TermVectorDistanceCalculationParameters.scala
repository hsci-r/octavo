package parameters

import com.koloboke.collect.map.LongDoubleMap
import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.{Filtering, Normalization}

class TermVectorDistanceCalculationParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], queryMetadata: QueryMetadata) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val distanceOpt = p.get("distance").map(v => DistanceMetric.withName(v.head.toUpperCase))
  /** distance metric used for term vector comparisons */
  val distanceMetric: DistanceMetric = distanceOpt.getOrElse(DistanceMetric.COSINE)
  
  def distance(t1: LongDoubleMap, t2: LongDoubleMap): Double = distanceMetric(t1, t2, this)
  
  private val normalizationOpt = p.get("normalization").map(v => Normalization.withName(v.head.toUpperCase))
  /** are vectors normalized before distance calculation? */
  val normalization: Normalization = normalizationOpt.getOrElse(Normalization.NONE)
  
  private val filteringOpt = p.get("filtering").map(v => Filtering.withName(v.head.toUpperCase))
  /** does distance calculation only operate on dimensions where both vectors have a value? (does not apply to DICE/JACCARD metrics)*/
  val filtering: Filtering = filteringOpt.getOrElse(Filtering.EITHER)
  
  def toJson: JsObject = Json.obj(
    prefix+"normalization"+suffix->normalization.entryName,
    prefix+"filtering"+suffix->filtering.entryName,
    prefix+"distance"+suffix->distanceMetric.entryName
  )
  queryMetadata.json = queryMetadata.json ++ toJson
}