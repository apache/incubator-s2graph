package controllers

import util.TestDataLoader
import play.api.mvc.{ Action, Controller, Result }
import play.api.libs.json.Json


object TestController extends Controller  {

//  def getRandomId(friendCount: Int) = Action { request =>
//    val idOpt = TestDataLoader.randomId(friendCount)
//    val id = idOpt.getOrElse(-1)
//    Ok(s"${id}")
//  }
  import ApplicationController._
  
  def getRandomId() = withHeader { request =>
    val id = TestDataLoader.randomId
    Ok(s"${id}")
  }
  
}