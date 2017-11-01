package controllers

import javax.inject.{Inject, Singleton}

import play.api.libs.json.Json
import play.api.mvc.InjectedController
import services.IndexAccessProvider

@Singleton
class RootController @Inject()(iap: IndexAccessProvider) extends InjectedController {
  
  def indices() = Action {
    Ok(Json.prettyPrint(iap.toJson)).as(JSON)
  }  
}