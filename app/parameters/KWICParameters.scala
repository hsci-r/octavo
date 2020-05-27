package parameters

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class KWICParameters()(implicit request: Request[AnyContent], ia: IndexAccess, queryMetadata: QueryMetadata) extends ContextParameters {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields: Seq[String] = p.getOrElse("field", Seq.empty)
  /** context level when sorting */
  val sortContextLevel = p.get("sortContextLevel").map(v => ContextLevel.withName(v.head.toUpperCase)).getOrElse(ContextLevel.WORD)
  private val distanceDefinition = "(-?\\d+)([ADad]?)([ISis]?)".r
  val sortContextDistances = p.getOrElse("sortContextDistance",Seq.empty).map{
    case distanceDefinition(distance,direction,caseSensitive) => (distance.toInt,direction match {
      case "A" | "a" | "" => SortDirection.ASC
      case "D" | "d" => SortDirection.DESC
      case sd => SortDirection.withName(sd.toUpperCase)
    }, caseSensitive match {
      case "I" | "i" | "insensitive" | "ignore" | "" => false
      case "S" | "s" | "sensitive" => true
    })
  }
  val sortContextDirections = sortContextDistances.map(_._2)
  val sortContextCaseSensitivities = sortContextDistances.map(_._3)
  val sortContextDistancesByDistance = sortContextDistances.zipWithIndex.sortBy(_._1._1).map(p => (p._1._1,p._2))
  /** how many results to skip */
  val offset: Int = p.get("offset").map(_.head.toInt).getOrElse(0)
  private val fullJson = Json.obj(
    "offset" -> offset,
    "sortContextLevel" -> sortContextLevel.entryName,
    "sortContextDistances" -> sortContextDistances,
    "fields"->fields
  )
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.contains(pa._1 match {
    case "fields" => "field"
    case "sortContextDistances" => "sortContextDistance"
    case a => a
  })))
}
