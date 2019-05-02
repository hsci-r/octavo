package controllers

import javax.inject.{Inject, Singleton}
import parameters.{GeneralParameters, QueryMetadata, SortDirection}
import play.api.libs.json.Json
import services.{IndexAccessProvider, MetadataOpts, TermSort}

@Singleton
class IndexInfoController @Inject() (implicit iap: IndexAccessProvider, qc: QueryCache) extends AQueuingController(qc) {
  
  def info(index: String) = Action { implicit request =>
    implicit val ia = iap(index)
    val p = request.body.asFormUrlEncoded.getOrElse(request.queryString)
    val stats = p.get("stats").exists(v => v.head=="" || v.head.toBoolean)
    val quantiles = p.get("quantiles").exists(v => v.head=="" || v.head.toBoolean)
    val histograms = p.get("histograms").exists(v => v.head=="" || v.head.toBoolean)
    val from = p.get("from").map(_.head.toDouble).getOrElse(0.0)
    val to = p.get("to").map(_.head.toDouble).getOrElse(1.0)
    val byS = p.get("by").map(_.head).getOrElse("0.1")
    val maxTermsToStat = p.get("maxTermsToStat").map(_.head.toInt).getOrElse(0)
    val by = BigDecimal(byS)
    val sortTermsBy = p.get("sortTermsBy").map(s => TermSort.withName(s.head.toUpperCase)).getOrElse(TermSort.TERM)
    val termSortDirection = p.get("termSortDirection").map(_.head).map {
      case "A" | "a" => SortDirection.ASC
      case "D" | "d" => SortDirection.DESC
      case sd => SortDirection.withName(sd.toUpperCase)
    }.getOrElse(SortDirection.ASC)
    implicit val qm = new QueryMetadata(Json.obj(
      "stats"->stats,
      "quantiles"->quantiles,
      "histograms"->histograms,
      "from"->from,
      "to"->to,
      "by"->byS,
      "maxTermsToStat"->maxTermsToStat,
      "sortTermsBy"->sortTermsBy,
      "termSortDirection"->termSortDirection))
    qm.longRunning = false
    val gp = new GeneralParameters()
    getOrCreateResult("indexInfo", ia.indexMetadata, qm, gp.force, gp.pretty, () => {
      // will not be called because longRunning = false
    }, () => {
      Left(iap(index).indexMetadata.toJson(iap(index),MetadataOpts(stats,quantiles,histograms,from,to,by,maxTermsToStat,sortTermsBy,termSortDirection,byS.length-2)))
    })
  }  
}