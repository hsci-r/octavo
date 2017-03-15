package parameters

import play.api.mvc.Request
import play.api.mvc.AnyContent
import org.apache.lucene.index.LeafReader
import java.util.Collections
import org.apache.lucene.index.DocValues
import services.IndexAccess
import play.api.libs.json.Json

case class QueryReturnParameters(implicit request: Request[AnyContent], ia: IndexAccess) {
  import ia._
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val termVectorFields: Seq[String] = p.get("field").getOrElse(Seq.empty).filter(ia.indexMetadata.termVectorFields.contains(_))
  val sortedDocValuesFields: Seq[String] = p.get("field").getOrElse(Seq.empty).filter(ia.indexMetadata.sortedDocValuesFields.contains(_))
  val storedSingularFields: Seq[String] = p.get("field").getOrElse(Seq.empty) filter(ia.indexMetadata.storedSingularFields.contains(_))
  val storedMultiFields: Seq[String] = p.get("field").getOrElse(Seq.empty) filter(ia.indexMetadata.storedMultiFields.contains(_))
  val numericDocValuesFields: Seq[String] = p.get("field").getOrElse(Seq.empty).filter(ia.indexMetadata.numericDocValuesFields.contains(_))
  /** return explanations and norms in search */
  val returnNorms: Boolean = p.get("returnNorms").exists(v => v(0)=="" || v(0).toBoolean)
  val returnMatches: Boolean = p.get("returnMatches").exists(v => v(0)=="" || v(0).toBoolean)
  private val sumScalingOpt = p.get("sumScaling").map(v => SumScaling.withName(v(0).toUpperCase))
  val sumScaling: SumScaling = sumScalingOpt.getOrElse(SumScaling.TTF)
  val limit: Int = p.get("limit").map(_(0).toInt).getOrElse(20)
  override def toString() = s"limit:$limit, sumScaling:$sumScaling, fields:${termVectorFields++sortedDocValuesFields++storedSingularFields++storedMultiFields++numericDocValuesFields}, returnMatches: $returnMatches, returnNorms: $returnNorms"
  def toJson() = Json.obj("limit"->limit,"sumScaling"->sumScaling.entryName,"fields"->(termVectorFields++sortedDocValuesFields++storedSingularFields++storedMultiFields++numericDocValuesFields),"returnMatches"->returnMatches,"returnNorms"->returnNorms)
}
