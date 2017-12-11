package parameters

import groovy.lang.GroovyShell
import play.api.libs.json.Json
import play.api.mvc.{AnyContent, Request}
import services.IndexAccess

case class GroupingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent], ia: IndexAccess) {
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val attrs = p.getOrElse("attr", Seq.empty)
  val attrLengths = p.getOrElse("attrLength", Seq.empty).map(_.toInt)
  private val attrTransformerS = p.get("attrTransformer").map(_.head)
  val attrTransformer = attrTransformerS.map(apScript => new GroovyShell().parse(apScript))
  private val grouperS = p.get("grouper").map(_.head)
  val grouper = grouperS.map(apScript => new GroovyShell().parse(apScript))
  private val maxDocFreqOpt = p.get(prefix+"maxDocFreq"+suffix).map(_.head.toInt)
  /** maximum total document frequency of term to be added to the term vector */
  val maxDocFreq: Int = maxDocFreqOpt.getOrElse(Int.MaxValue)
  def toJson = Json.obj(prefix+"attrs"+suffix->attrs,prefix+"attrLengths"+suffix->attrLengths,prefix+"attrTransformer"+suffix->attrTransformerS,prefix+"grouper"+suffix->grouperS)
  def isDefined = !attrs.isEmpty || grouper.isDefined
}

