package parameters

import play.api.mvc.Request
import play.api.mvc.AnyContent
import org.apache.lucene.index.LeafReader
import java.util.Collections
import org.apache.lucene.index.DocValues
import services.IndexAccess

case class QueryReturnParameters(implicit request: Request[AnyContent]) {
  import QueryReturnParameters._
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val termVectorFields: Seq[String] = p.get("field").getOrElse(Seq.empty).filter(isTermVectorField)
  val sortedDocValuesFields: Seq[String] = p.get("field").getOrElse(Seq.empty).filter(isSortedDocValuesField)
  val storedSingularFields: Seq[String] = p.get("field").getOrElse(Seq.empty) filter(isStoredSingularField)
  val storedMultiFields: Seq[String] = p.get("field").getOrElse(Seq.empty) filter(isStoredMultiField)
  val numericDocValuesFields: Seq[String] = p.get("field").getOrElse(Seq.empty).filter(isNumericDocValuesField)
  /** return explanations and norms in search */
  val returnNorms: Boolean = p.get("returnNorms").exists(v => v(0)=="" || v(0).toBoolean)
  val returnMatches: Boolean = p.get("returnMatches").exists(v => v(0)=="" || v(0).toBoolean)
  private val sumScalingOpt = p.get("sumScaling").map(v => SumScaling.withName(v(0).toUpperCase))
  val sumScaling: SumScaling = sumScalingOpt.getOrElse(SumScaling.TTF)
  override def toString() = s"sumScaling:$sumScaling, termVectorFields:$termVectorFields, sortedDocValuesFields:$sortedDocValuesFields, storedSingularFields:$storedSingularFields, storedMultiFields:$storedMultiFields, numericDocValuesFields:$numericDocValuesFields, returnMatches: $returnMatches, returnNorms: $returnNorms"
}

object QueryReturnParameters {
  import IndexAccess._
  def isTermVectorField(field: String) = field match {
    case "containsGraphicOfType" => true
    case _ => false
  }
  def isSortedDocValuesField(field: String) = field match {
    case "documentPartType" | "headingLevel" | "collectionId" | "documentID" | "ESTCID" | "language" | "module" => true
    case _ => false
  }
  def isStoredSingularField(field: String) = field match {
    case "fullTitle" | "content" => true
    case _ => false
  }
  def isStoredMultiField(field: String) = field match {
    case "containsGraphicOfType" => true
    case _ => false
  }
  def isNumericDocValuesField(field: String) = field match {
    case "pubDateStart" | "pubDateEnd" | "totalPages" | "documentLength" | "totalParagraphs" | "partID" | "paragraphID" | "sectionID" | "contentTokens" | "" => true
    case _ => false      
  }
  def getter(lr: LeafReader, field: String): (Int) => Iterable[String] = {
    if (isStoredSingularField(field) || isStoredMultiField(field)) {
      val fieldS = Collections.singleton(field)
      return (doc: Int) => lr.document(doc,fieldS).getValues(field).toSeq
    }
    if (isSortedDocValuesField(field)) {
      val dvs = DocValues.getSorted(lr, field)
      return (doc: Int) => Seq(dvs.get(doc).utf8ToString())
    }
    if (isNumericDocValuesField(field)) {
      val dvs = DocValues.getNumeric(lr, field)
      return (doc: Int) => Seq(""+dvs.get(doc))
    }
    if (isTermVectorField(field))
      return (doc: Int) => lr.getTermVector(doc, field).asBytesRefIterable().map(_.utf8ToString)
    return null
  }
}