package parameters

import play.api.libs.json.Json
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

case class QueryReturnParameters()(implicit request: Request[AnyContent], ia: IndexAccess) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields: Seq[String] = p.getOrElse("field", Seq.empty)
  /** return explanations for matches in search */
  val returnExplanations: Boolean = p.get("returnExplanations").exists(v => v.head=="" || v.head.toBoolean)
  val returnMatches: Boolean = p.get("returnMatches").exists(v => v.head=="" || v.head.toBoolean)
  private val sumScalingStringOpt = p.get("sumScaling").map(v => v.head.toUpperCase)
  private val sumScalingString = sumScalingStringOpt.getOrElse("TTF")
  private val smoothingOpt = p.get("smoothing").map(_.head.toDouble)
  /** Laplace smoothing to use */
  val smoothing = smoothingOpt.getOrElse(0.0)
  /** sum scaling to use */
  val sumScaling = SumScaling.get(sumScalingString, smoothing)
  /** how many results to return at maximum */
  val limit: Int = p.get("limit").map(_.head.toInt).getOrElse(20)
  /** context level when returning matches */
  val contextLevel = p.get("contextLevel").map(v => ContextLevel.withName(v.head.toUpperCase)).getOrElse(ContextLevel.SENTENCE)
  /** how much to expand returned match context */
  val contextExpand = p.get("contextExpand").map(v => v.head.toInt).getOrElse(0)
  def toJson = Json.obj("limit"->limit,"contextLevel"->contextLevel.entryName,"contextExpand"->contextExpand,"smoothing"->smoothing,"sumScaling"->sumScalingString,"fields"->fields,"returnMatches"->returnMatches,"returnExplanations"->returnExplanations)
}
