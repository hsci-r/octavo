package controllers

import javax.inject.{Inject, Singleton}

import play.api.libs.json.Json
import play.api.mvc.InjectedController
import services.IndexAccessProvider

@Singleton
class IndexInfoController @Inject() (iap: IndexAccessProvider) extends InjectedController {
  
  def info(index: String) = Action {
    Ok(Json.prettyPrint(iap(index).indexMetadata.toJson(iap(index)))).as(JSON)
  }  
}