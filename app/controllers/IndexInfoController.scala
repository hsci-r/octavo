package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.InjectedController
import services.IndexAccessProvider

@Singleton
class IndexInfoController @Inject() (iap: IndexAccessProvider) extends InjectedController {
  
  def info(index: String) = Action {
    Ok(iap(index).indexMetadata.toJson).as(JSON)
  }  
}