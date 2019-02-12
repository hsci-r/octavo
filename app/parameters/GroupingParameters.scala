package parameters

import controllers.RunScriptController
import groovy.lang.GroovyShell
import play.api.libs.json.Json
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
  val groupByMatch: Boolean = p.get("groupByMatch").exists(v => v.head=="" || v.head.toBoolean)
  private val matchTransformerS = p.get("matchTransformer").map(_.head)
  val matchTransformer = matchTransformerS.map(apScript => new GroovyShell(RunScriptController.compilerConfiguration).parse(apScript))
  val matchLength = p.get("matchLength").map(_.head.toInt)
  private val myJson = Json.obj(prefix+"fields"+suffix->fields,prefix+"fieldLengths"+suffix->fieldLengths,prefix+"fieldTransformer"+suffix->fieldTransformerS,prefix+"grouper"+suffix->grouperS,prefix+"groupByMatch"+suffix->groupByMatch,prefix+"matchTransformer"+suffix->matchTransformerS,prefix+"matchLength"+suffix->matchLength)
  override def toJson = super.toJson ++ myJson
  def isDefined = fields.nonEmpty || grouper.isDefined || groupByMatch
  queryMetadata.json = queryMetadata.json ++ myJson
}

