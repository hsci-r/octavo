package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}

class TermVectorDimensionalityReductionParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], queryMetadata: QueryMetadata) extends TermVectorDistanceCalculationParameters(prefix, suffix)(request, queryMetadata) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val dimensionsOpt = p.get(prefix+"dimensions"+suffix).map(_.head.toInt)
  /** amount of dimensions for dimensionally reduced term vector coordinates */
  val dimensions: Int = dimensionsOpt.getOrElse(0)
  
  private val dimensionalityReductionOpt = p.get(prefix+"dimReduct"+suffix).map(v => DimensionalityReduction.withName(v.head.toUpperCase))
  val dimensionalityReduction: DimensionalityReduction = dimensionalityReductionOpt.getOrElse(DimensionalityReduction.SMDS)
  
  val tsnePerplexity: Double = 20.0
  val tsneMaxIter: Int = 1000
  val tsneUsePCA: Boolean = true
  val tsneTheta: Double = 0.5

  override def toJson: JsObject = super.toJson ++
    Json.obj(
      prefix+"dimensions"+suffix->dimensions,
      prefix+"dimReduct"+suffix->(""+dimensionalityReduction)
    )
  queryMetadata.json = queryMetadata.json ++ toJson
}