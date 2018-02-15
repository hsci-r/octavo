package controllers

import javax.inject.{Inject, Singleton}

import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.InjectedController
import services.IndexAccessProvider

@Singleton
class RootController @Inject()(iap: IndexAccessProvider, configuration: Configuration) extends InjectedController {

  private final val version = configuration.get[String]("app.version")

  def indices() = Action {
    Ok(Json.prettyPrint(Json.obj("octavoVersion"->version,"indices"->iap.toJson))).as(JSON)
  }  
}