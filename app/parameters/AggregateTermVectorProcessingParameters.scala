package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}

class AggregateTermVectorProcessingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], queryMetadata: QueryMetadata) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val smoothingOpt = p.get(prefix+"smoothing"+suffix).map(_.head.toDouble)
  /** Laplace smoothing to use */
  val smoothing = smoothingOpt.getOrElse(0.0)
  private val sumScalingStringOpt = p.get(prefix+"sumScaling"+suffix).map(v => v.head.toUpperCase)
  private val sumScalingString = sumScalingStringOpt.getOrElse("TTF")
  /** sum scaling to use */
  val sumScaling = SumScaling.get(sumScalingString, smoothing)
  private val minSumFreqOpt = p.get(prefix+"minSumFreq"+suffix).map(_.head.toInt)
  /** minimum sum frequency of term to filter resulting term vector */
  val minSumFreq: Int = minSumFreqOpt.getOrElse(1)
  private val maxSumFreqOpt = p.get(prefix+"maxSumFreq"+suffix).map(_.head.toInt)
  /** maximum sum frequency of term to filter resulting term vector */
  val maxSumFreq: Int = maxSumFreqOpt.getOrElse(Int.MaxValue)
  final def matches(sumFreq: Int): Boolean = {
    (minSumFreq == 1 && maxSumFreq == Int.MaxValue) || (minSumFreq <= sumFreq && maxSumFreq >= sumFreq)
  }
  def toJson: JsObject = Json.obj(
    prefix+"smoothing"+suffix->smoothing,
    prefix+"sumScaling"+suffix->sumScalingString,
    prefix+"minSumFreq"+suffix->minSumFreq,
    prefix+"maxSumFreq"+suffix->maxSumFreq,
  )
  queryMetadata.json = queryMetadata.json ++ toJson
}