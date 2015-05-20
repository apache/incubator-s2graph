package test.controllers


//package kgraph
//import scala.concurrent.Await
//import scala.concurrent._
//import scala.concurrent.duration.Duration
//import scala.concurrent.duration._
//import org.specs2.mutable.Specification
//import play.api.test.FakeApplication
//import play.api.test.Helpers._
//import scala.concurrent.duration._
//import java.util.concurrent.TimeUnit
//import scala.util.Failure
//import scala.util.Success
//
//class FutureHelpersSpec extends Specification {
//
//  "TimeoutFuture" should {
//    "timeout with cancellation" in {
//      running(FakeApplication()) {
//        implicit val ex = ExecutionContext.Implicits.global
//        println(s"Start: ${System.currentTimeMillis()}")
//        val f = TimeoutFuture({
//          Thread.sleep(60000)
//          "Finised"
//        }, Duration(1, TimeUnit.SECONDS))({ "Timeout" })
//        
//        
//        f.onComplete {
//          case Failure(ex) =>
//            println(s"Failure: $ex: ${System.currentTimeMillis()}")
//          case Success(ret) =>
//            println(s"Success: $ret: ${System.currentTimeMillis()}")
//        }
//        val ret = Await.result(f, Duration.apply(30, TimeUnit.SECONDS))
//        println(ret)
//        true
//      }
//    }
//  }
//}