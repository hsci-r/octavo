package parameters

import play.api.libs.json.Json
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class KWICParameters()(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) extends ContextParameters {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields: Seq[String] = p.getOrElse("field", Seq.empty)
  /** context level when sorting */
  val sortContextLevel = p.get("sortContextLevel").map(v => ContextLevel.withName(v.head.toUpperCase)).getOrElse(ContextLevel.WORD)
  val sortContextDistances = p.getOrElse("sortContextDistance",Seq.empty).map(_.toInt)
  val sortContextDirections = p.getOrElse("sortContextDirection",Seq.empty).map(sd => SortDirection.withName(sd.toUpperCase))
  val sortContextDistancesByDistance = sortContextDistances.zipAll(sortContextDirections,0,SortDirection.ASC).zipWithIndex.sortBy(_._1._1).map(p => (p._1._1,p._1._2,p._2))
  /** how many results to return at maximum */
  val limit: Int = p.get("limit").map(_.head.toInt).getOrElse(20)
  /** how many results to skip */
  val offset: Int = p.get("offset").map(_.head.toInt).getOrElse(0)
  private val myJson = Json.obj(
    "limit"->limit,
    "offset" -> offset,
    "sortContextLevel" -> sortContextLevel.entryName,
    "sortContextDistances" -> sortContextDistances,
    "sortContextDirections" -> sortContextDirections,
    "fields"->fields
  )
  override def toJson = super.toJson ++ myJson
  queryMetadata.json = queryMetadata.json ++ myJson
}
