package controllers

import com.daumkakao.s2graph.core.GraphUtil
import com.daumkakao.s2graph.core.mysqls._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future

/**
 * Created by shon on 8/5/15.
 */
object BucketController extends Controller {

  def experiment(serviceName: String,
                 experimentKey: String,
                 cookie: String) = Action.async { request =>
    for {
      serviceOpt <- Future.successful(Service.findByName(serviceName))
      service <- serviceOpt
      exp <- ServiceExperiment.find(service.id.get, experimentKey)
      mod = GraphUtil.murmur3(cookie) % exp.totalMod
      bucket <- exp.bucketModMap.get(mod)
      response <- bucket.call(cookie)
    } yield {
      Ok(Json.obj("impressionId" -> bucket.impressionId, "results" -> response.body))
    }
  }
}
