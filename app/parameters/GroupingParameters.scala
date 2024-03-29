package parameters

import controllers.RunScriptController
import groovy.lang.GroovyShell
import play.api.libs.json.{JsObject, Json}
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

class GroupingParameters(prefix: String = "", suffix: String = "")(implicit protected val request: Request[AnyContent], protected val ia: IndexAccess, protected val queryMetadata: QueryMetadata) extends ContextParameters(prefix,suffix) with SortParameters {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val fields = p.getOrElse("field", Seq.empty)
  val fieldLengths = p.getOrElse("fieldLength", Seq.empty).map(_.toInt)
  private val fieldTransformerS = p.get("fieldTransformer").map(_.head)
  val fieldTransformer = fieldTransformerS.map(apScript => new GroovyShell(RunScriptController.compilerConfiguration).parse(apScript))
  private val grouperS = p.get("grouper").map(_.head)
  val grouper = grouperS.map(apScript => new GroovyShell(RunScriptController.compilerConfiguration).parse(apScript))
  val groupByMatchTerm: Boolean = p.get("groupByMatchTerm").exists(v => v.head=="" || v.head.toBoolean)
  val groupByMatch: Boolean = p.get("groupByMatch").exists(v => v.head=="" || v.head.toBoolean)
  private val matchTransformerS = p.get("matchTransformer").map(_.head)
  val matchTransformer = matchTransformerS.map(apScript => new GroovyShell(RunScriptController.compilerConfiguration).parse(apScript))
  val matchLength = p.get("matchLength").map(_.head.toInt)
  private val fullJson = Json.obj(prefix+"fields"+suffix->fields,prefix+"fieldLengths"+suffix->fieldLengths,prefix+"fieldTransformer"+suffix->fieldTransformerS,prefix+"grouper"+suffix->grouperS,prefix+"groupByMatch"+suffix->groupByMatch,prefix+"groupByMatchTerm"+suffix->groupByMatchTerm,prefix+"matchTransformer"+suffix->matchTransformerS,prefix+"matchLength"+suffix->matchLength)
  def isDefined = fields.nonEmpty || grouper.isDefined || groupByMatch || groupByMatchTerm
  queryMetadata.fullJson = queryMetadata.fullJson ++ fullJson
  queryMetadata.nonDefaultJson = queryMetadata.nonDefaultJson ++ JsObject(fullJson.fields.filter(pa => p.contains(prefix + (pa._1.drop(prefix.length).dropRight(suffix.length) match {
    case "fields" => "field"
    case "fieldLengths" => "fieldLength"
    case a => a
  }) + suffix)))
}

