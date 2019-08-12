package controllers

import javax.inject.{Inject, Singleton}
import play.api.libs.json.Json
import play.api.mvc.InjectedController

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@Singleton
class QueryStatusController @Inject()(qc: QueryCache) extends InjectedController {

  def result(key: String) = Action {
    val (_,tf) = qc.files(key)
    if (!tf.exists()) NotFound("\"No query with this key found either running or completed.\"").as(HTML)
    else Option(qc.runningQueries.get(key)).map(p => Await.result(p._2, Duration.Inf)).getOrElse({
      import scala.concurrent.ExecutionContext.Implicits.global
      Ok.sendFile(tf).as(JSON)
    })
  }

  def status(key: String) = Action {
    Option(qc.runningQueries.get(key)) match {
      case None => NotFound("<html><body><h1>Not Running</h1>Statistics could not be fetched because the query is not running. Maybe it is already finished with a <a href=\"../result/"+key+"\">result</a> available?</body></html>").as(HTML)
      case Some((queryMetadata,_)) => Ok(Json.prettyPrint(Json.obj( "status"->queryMetadata.status, "parameters"->queryMetadata.nonDefaultJson,"fullParameters"->queryMetadata.fullJson))).as(JSON)
    }
  }
}