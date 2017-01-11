package parameters

import org.apache.lucene.search.Query
import play.api.mvc.Request
import play.api.mvc.AnyContent
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.AutomatonQuery
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.util.automaton.Automata
import services.IndexAccess

case class QueryParameters(prefix: String = "", suffix: String = "")(implicit request: Request[AnyContent]) {
  protected val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
  val query: Option[String] = p.get(prefix+"query"+suffix).map(v => if (!v(0).startsWith("<")) "<paragraph|"+v(0)+"|paragraph>" else v(0))
  /** minimum query score (by default term match frequency) for doc to be included in query results */
  val minScore: Float = p.get(prefix+"minScore"+suffix).map(_(0).toFloat).getOrElse(0.0f)
  override def toString() = s"${prefix}query$suffix:$query, ${prefix}minScore$suffix: $minScore"
}