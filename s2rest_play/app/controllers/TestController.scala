package controllers

import play.api.mvc.{Action, Controller}
import util.TestDataLoader

import scala.concurrent.Future


object TestController extends Controller {
  import ApplicationController._

  def getRandomId() = withHeader(parse.anyContent) { request =>
    val id = TestDataLoader.randomId
    Ok(s"${id}")
  }

  def pingAsync() = Action.async(parse.json) { request =>
    Future.successful(Ok("Pong\n"))
  }

  def ping() = Action(parse.json) { request =>
    Ok("Pong\n")
  }
}
