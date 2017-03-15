package parameters

import play.api.mvc.Request
import play.api.mvc.AnyContent
import org.apache.lucene.index.IndexReader
import play.api.libs.json.Json

case class LocalTermVectorProcessingParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) {
  import services.IndexAccess.{totalTermFreq,docFreq,termOrdToTerm}
  private val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  private val minFreqInDocOpt = p.get(prefix+"minFreqInDoc"+suffix).map(_(0).toLong)
  /** minimum per document frequency of term to be added to the term vector */
  val minFreqInDoc: Long = minFreqInDocOpt.getOrElse(1l)
  private val maxFreqInDocOpt = p.get(prefix+"maxFreqInDoc"+suffix).map(_(0).toLong)
  /** maximum per document frequency of term to be added to the term vector */
  val maxFreqInDoc: Long = maxFreqInDocOpt.getOrElse(Long.MaxValue)
  private def freqInDocMatches(freq: Long): Boolean = freq>=minFreqInDoc && freq<=maxFreqInDoc
  private val localScalingOpt = p.get(prefix+"localScaling"+suffix).map(v => LocalTermVectorScaling.withName(v(0).toUpperCase))
  /** per-doc term vector scaling: absolute, min (w.r.t query term) or flat (1) */
  val localScaling: LocalTermVectorScaling.Value = localScalingOpt.getOrElse(LocalTermVectorScaling.ABSOLUTE)
  private val minTotalTermFreqOpt = p.get(prefix+"minTotalTermFreq"+suffix).map(_(0).toLong)
  /** minimum total term frequency of term to be added to the term vector */
  val minTotalTermFreq: Long = minTotalTermFreqOpt.getOrElse(1l)
  private val maxTotalTermFreqOpt = p.get(prefix+"maxTotalTermFreq"+suffix).map(_(0).toLong)
  /** maximum total term frequency of term to be added to the term vector */
  val maxTotalTermFreq: Long = maxTotalTermFreqOpt.getOrElse(Long.MaxValue)
  private def totalTermFreqMatches(ir: IndexReader, term: Long): Boolean = {
    if (minTotalTermFreq == 1 && maxTotalTermFreq==Long.MaxValue) return true
    val ttfr = totalTermFreq(ir, term)
    return ttfr>=minTotalTermFreq && ttfr<=maxTotalTermFreq
  }
  private val minDocFreqOpt = p.get(prefix+"minDocFreq"+suffix).map(_(0).toInt)
  /** minimum total document frequency of term to be added to the term vector */
  val minDocFreq: Int = minDocFreqOpt.getOrElse(1)
  private val maxDocFreqOpt = p.get(prefix+"maxDocFreq"+suffix).map(_(0).toInt)
  /** maximum total document frequency of term to be added to the term vector */
  val maxDocFreq: Int = maxDocFreqOpt.getOrElse(Int.MaxValue)
  private def docFreqMatches(ir: IndexReader, term: Long): Boolean = {
    if (minDocFreq == 1 && maxDocFreq==Int.MaxValue) return true
    val dfr = docFreq(ir, term)
    return dfr>=minDocFreq && dfr<=maxDocFreq
  }
  private val minTermLengthOpt = p.get(prefix+"minTermLength"+suffix).map(_(0).toInt)
  /** minimum length of term to be included in the term vector */
  val minTermLength: Int = minTermLengthOpt.getOrElse(1)
  private val maxTermLengthOpt = p.get(prefix+"maxTermLength"+suffix).map(_(0).toInt)
  /** maximum length of term to be included in the term vector */
  val maxTermLength: Int = maxTermLengthOpt.getOrElse(Int.MaxValue)
  private def termLengthMatches(ir: IndexReader, term: Long): Boolean = {
    if (minTermLength == 1 && maxTermLength == Int.MaxValue) return true
    val terms = termOrdToTerm(ir, term)
    return terms.length>=minTermLength && terms.length<=maxTermLength
  }
  final def matches(ir: IndexReader, term: Long, freq: Long): Boolean =
    freqInDocMatches(freq) && docFreqMatches(ir,term) && totalTermFreqMatches(ir,term) && termLengthMatches(ir,term)
  val defined: Boolean = localScalingOpt.isDefined || minFreqInDocOpt.isDefined || maxFreqInDocOpt.isDefined || minTotalTermFreqOpt.isDefined || maxTotalTermFreqOpt.isDefined || minDocFreqOpt.isDefined || maxDocFreqOpt.isDefined || minTermLengthOpt.isDefined || maxTermLengthOpt.isDefined
  override def toString() = s"${prefix}localScaling$suffix:$localScaling, ${prefix}minTotalTermFreq$suffix:$minTotalTermFreq, ${prefix}maxTotalTermFreq$suffix:$maxTotalTermFreq, ${prefix}minDocFreq$suffix:$minDocFreq, ${prefix}maxDocFreq$suffix:$maxDocFreq, ${prefix}minFreqInDoc$suffix:$minFreqInDoc, ${prefix}maxFreqInDoc$suffix:$maxFreqInDoc, ${prefix}minTermLength$suffix:$minTermLength, ${prefix}maxTermLength$suffix:$maxTermLength"
  def toJson() = Json.obj(prefix+"localScaling"+suffix->localScaling, prefix+"minTotalTermFreq"+suffix->minTotalTermFreq, prefix+"maxTotalTermFreq"+suffix->maxTotalTermFreq, prefix+"minDocFreq"+suffix->minDocFreq, prefix+"maxDocFreq"+suffix->maxDocFreq, prefix+"minFreqInDoc"+suffix -> minFreqInDoc, prefix+"maxFreqInDoc"+suffix -> maxFreqInDoc, prefix + "minTermLength" + suffix -> minTermLength, prefix + "maxTermLength" + suffix -> maxTermLength)
}

