package parameters

import play.api.mvc.Request
import play.api.mvc.AnyContent
import play.api.libs.json.JsObject
import play.api.libs.json.Json

case class AggregateTermVectorProcessingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val smoothingOpt = p.get(prefix+"smoothing"+suffix).map(_(0).toDouble)
  /** Laplace smoothing to use */
  val smoothing = smoothingOpt.getOrElse(2.0)
  private val sumScalingStringOpt = p.get(prefix+"sumScaling"+suffix).map(v => v(0).toUpperCase)
  private val sumScalingString = sumScalingStringOpt.getOrElse("TTF")
  /** sum scaling to use */
  val sumScaling = SumScaling.get(sumScalingString, smoothing)
  private val minSumFreqOpt = p.get(prefix+"minSumFreq"+suffix).map(_(0).toInt)
  /** minimum sum frequency of term to filter resulting term vector */
  val minSumFreq: Int = minSumFreqOpt.getOrElse(1)
  private val maxSumFreqOpt = p.get(prefix+"maxSumFreq"+suffix).map(_(0).toInt)
  /** maximum sum frequency of term to filter resulting term vector */
  val maxSumFreq: Int = maxSumFreqOpt.getOrElse(Int.MaxValue)
  final def matches(sumFreq: Int): Boolean = {
    (minSumFreq == 1 && maxSumFreq == Int.MaxValue) || (minSumFreq <= sumFreq && maxSumFreq >= sumFreq)
  }
  val limitOpt = p.get(prefix+"limit"+suffix).map(_(0).toInt)
  val limit: Int = limitOpt.getOrElse(20)
  private val mdsDimensionsOpt = p.get("mdsDimensions").map(_(0).toInt)
  /** amount of dimensions for dimensionally reduced term vector coordinates */
  val mdsDimensions: Int = mdsDimensionsOpt.getOrElse(0)    
  private val distanceOpt = p.get("distance").map(v => DistanceMetric.withName(v(0).toUpperCase))
  /** distance metric used for term vector comparisons */
  val distance: DistanceMetric = distanceOpt.getOrElse(DistanceMetric.COSINE)
  val defined: Boolean = sumScalingStringOpt.isDefined || minSumFreqOpt.isDefined || maxSumFreqOpt.isDefined || limitOpt.isDefined || mdsDimensionsOpt.isDefined || distanceOpt.isDefined
  def toJson(): JsObject = Json.obj(prefix+"smoothing"+suffix->smoothing,prefix+"sumScaling"+suffix->sumScalingString,prefix+"minSumFreq"+suffix->minSumFreq,prefix+"maxSumFreq"+suffix->maxSumFreq,prefix+"limit"+suffix->limit, prefix+"mdsDimensions"+suffix->mdsDimensions, prefix+"distance"+suffix->distance.entryName) 
}