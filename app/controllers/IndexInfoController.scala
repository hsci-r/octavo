package controllers

import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import play.api.libs.json.JsValue
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.search.Scorer
import org.apache.lucene.index.LeafReaderContext
import play.api.libs.json.Json
import javax.inject.Inject
import org.apache.lucene.queryparser.classic.QueryParser
import javax.inject.Named
import services.IndexAccess
import parameters.SumScaling
import play.api.libs.json.JsObject
import scala.collection.JavaConverters._
import com.tdunning.math.stats.TDigest
import org.apache.commons.lang3.SerializationUtils
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import services.IndexAccessProvider
import play.api.mvc.InjectedController

@Singleton
class IndexInfoController @Inject() (iap: IndexAccessProvider) extends InjectedController {
  
  def info(index: String) = Action {
    Ok(iap(index).indexMetadata.toJson).as(JSON)
  }  
}