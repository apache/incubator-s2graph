//package com.kakao.s2graph.core.storage.hbase
//
//import com.kakao.s2graph.core.Graph
//import com.typesafe.config.ConfigFactory
//
//import org.apache.hadoop.hbase.util.Bytes
//import org.hbase.async.GetRequest
//import org.scalatest.{FunSuite, Matchers}
//
//import scala.concurrent.ExecutionContext
//
//class AsynchbaseQueryBuilderTest extends FunSuite with Matchers {
//
//  val dummyRequests = {
//    for {
//      id <- 0 until 1000
//    } yield {
//      new GetRequest("a", Bytes.toBytes(id))
//    }
//  }
//
//  implicit val ec = ExecutionContext.Implicits.global
//  val config = ConfigFactory.load()
//  val graph = new Graph(config)
//
//  val qb = new AsynchbaseQueryBuilder(graph.storage.asInstanceOf[AsynchbaseStorage])
//
//  test("test toCacheKeyBytes") {
//    val startedAt = System.nanoTime()
//
//    for {
//      i <- dummyRequests.indices
//      x = qb.toCacheKeyBytes(dummyRequests(i))
//    } {
//      for {
//        j <- dummyRequests.indices if i != j
//        y = qb.toCacheKeyBytes(dummyRequests(j))
//      } {
//        x should not equal y
//      }
//    }
//
//    dummyRequests.zip(dummyRequests).foreach { case (x, y) =>
//      val xHash = qb.toCacheKeyBytes(x)
//      val yHash = qb.toCacheKeyBytes(y)
//      //      println(xHash, yHash)
//      xHash should be(yHash)
//    }
//    val duration = System.nanoTime() - startedAt
//
//    println(s">> bytes: $duration")
//  }
//}
