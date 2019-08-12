package parameters

import play.api.libs.json._
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

trait WithToJson {
  def toJson: JsObject = Json.obj()
}

trait SortParameters extends WithToJson {
  protected val request: Request[AnyContent]
  protected val ia: IndexAccess
  protected val queryMetadata: QueryMetadata
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  protected val sortFields: Seq[String] = p.getOrElse("sort",Seq.empty)
  protected val sortDirections: Seq[SortDirection.Value] = p.getOrElse("sortDirection",Seq.empty).map {
    case "A" | "a" => SortDirection.ASC
    case "D" | "d" => SortDirection.DESC
    case sd => SortDirection.withName(sd.toUpperCase)
  }
  protected val sortCaseInsensitivities: Seq[Boolean] = p.getOrElse("sortCaseInsensitive",Seq.empty).map(sd => sd.toLowerCase match {
    case "i" | "insensitive" => true
    case "s" | "sensitive" => false
  })
  val sorts = sortFields.zipAll(sortDirections,ia.indexMetadata.contentField,SortDirection.ASC).zipAll(sortCaseInsensitivities,null,false).map(p => (p._1._1,p._1._2,p._2))
  private val fullJson = Json.obj(
    "sorts"->sortFields,
    "sortDirections"->sortDirections,
    "sortCaseInsensitivities"->sortCaseInsensitivities,
  )
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.get(pa._1 match {
    case "fields" => "field"
    case "sortCaseInsesitivities" => "sortCaseInsensitive"
    case "sorts" => "sort"
    case a => a
  }).isDefined))

  def compare(xs: Seq[JsValue], ys: Seq[JsValue]): Int = xs.view.zip(ys).zip(sorts).map{
    case ((x,y),(_,d,ci)) => compare(x,y,d,ci)
  }.find(_ != 0).getOrElse(0)

  def compare(x: JsValue, y: JsValue, direction: SortDirection.Value, ci: Boolean): Int = {
    val c = compare(x,y,ci)
    if (direction == SortDirection.DESC) -c else c
  }

  def compare(x: JsValue, y: JsValue, ci: Boolean): Int = x match {
    case null => if (y == null) 0 else 1
    case _ if y == null => -1
    case n : JsNumber => n.value.compare(y.asInstanceOf[JsNumber].value)
    case s : JsString => if (ci) s.value.toLowerCase.compare(y.asInstanceOf[JsString].value.toLowerCase) else s.value.compare(y.asInstanceOf[JsString].value)
    case a: JsArray => a.value.view.zipAll(y.asInstanceOf[JsArray].value,null,null).map(p => compare(p._1,p._2,ci)).find(_ != 0).getOrElse(0)
    case _ => 0
  }
}
