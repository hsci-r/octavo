package parameters

import play.api.mvc.Request
import play.api.mvc.AnyContent
import org.apache.lucene.index.LeafReader
import java.util.Collections
import org.apache.lucene.index.DocValues
import services.IndexAccess
import play.api.libs.json.Json

case class QueryReturnParameters()(implicit request: Request[AnyContent], ia: IndexAccess) {
  import ia._
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields: Seq[String] = p.get("field").getOrElse(Seq.empty)
  /** return explanations and norms in search */
  val returnNorms: Boolean = p.get("returnNorms").exists(v => v(0)=="" || v(0).toBoolean)
  val returnMatches: Boolean = p.get("returnMatches").exists(v => v(0)=="" || v(0).toBoolean)
  private val sumScalingStringOpt = p.get("sumScaling").map(v => v(0).toUpperCase)
  private val sumScalingString = sumScalingStringOpt.getOrElse("TTF")
  private val smoothingOpt = p.get("smoothing").map(_(0).toDouble)
  /** Laplace smoothing to use */
  val smoothing = smoothingOpt.getOrElse(0.0)
  /** sum scaling to use */
  val sumScaling = SumScaling.get(sumScalingString, smoothing)
  /** how many results to return at maximum */
  val limit: Int = p.get("limit").map(_(0).toInt).getOrElse(20)
  /** context level when returning matches */
  val contextLevel = p.get("contextLevel").map(v => ContextLevel.withName(v(0).toUpperCase)).getOrElse(ContextLevel.SENTENCE)
  /** how much to expand returned match context */
  val contextExpand = p.get("contextExpand").map(v => v(0).toInt).getOrElse(0)
  def toJson() = Json.obj("limit"->limit,"contextLevel"->contextLevel.entryName,"contextExpand"->contextExpand,"smoothing"->smoothing,"sumScaling"->sumScalingString,"fields"->fields,"returnMatches"->returnMatches,"returnNorms"->returnNorms)
}
