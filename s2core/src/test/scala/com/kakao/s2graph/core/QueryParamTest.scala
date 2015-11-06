package com.kakao.s2graph.core

import com.kakao.s2graph.core.types.{LabelWithDirection, HBaseType}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.GetRequest
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

/**
 * Created by shon on 7/31/15.
 */
class QueryParamTest extends FunSuite with Matchers with TestCommon {
//  val version = HBaseType.VERSION2
//  val testEdge = Management.toEdge(ts, "insert", "1", "10", labelNameV2, "out", Json.obj("is_blocked" -> true, "phone_number" -> "xxxx", "age" -> 20).toString)
//  test("EdgeTransformer toInnerValOpt") {
//
//    /** only labelNameV2 has string type output */
//    val jsVal = Json.arr(Json.arr("_to"), Json.arr("phone_number.$", "phone_number"), Json.arr("age.$", "age"))
//    val transformer = EdgeTransformer(queryParamV2, jsVal)
//    val convertedLs = transformer.transform(testEdge, None)
//
//    convertedLs(0).tgtVertex.innerId.toString == "10" shouldBe true
//    convertedLs(1).tgtVertex.innerId.toString == "phone_number.xxxx" shouldBe true
//    convertedLs(2).tgtVertex.innerId.toString == "age.20" shouldBe true
//    true
//  }

//  test("QueryParam toCacheKey bytes") {
//    val startedAt = System.nanoTime()
//    val queryParam = QueryParam(LabelWithDirection(1, 0))
//
//    val getRequests = for {
//      id <- (0 until 1000)
//    } yield {
//      new GetRequest("a".getBytes, Bytes.toBytes(id))
//    }
//
//    val rets = for {
//      i <- (0 until getRequests.size)
//      x = queryParam.toCacheKey(getRequests(i))
//    } yield {
//      val rets = for {
//        j <- (0 until getRequests.size) if i != j
//        y = queryParam.toCacheKey(getRequests(j))
//      } yield {
////          println(x, y)
//          x != y
//        }
//
//      rets.forall(identity)
//    }
//    println("\n\n")
//    val sames = getRequests.zip(getRequests).map { case (x, y) =>
//      val xHash = queryParam.toCacheKey(x)
//      val yHash = queryParam.toCacheKey(y)
////      println(xHash, yHash)
//      xHash == yHash
//    }.forall(identity)
//    val diffs = rets.forall(identity)
//    val duration = System.nanoTime() - startedAt
//
//    println(s">> bytes: $duration")
//    sames && diffs
//  }

//  test("QueryParam toCacheKey bytes2") {
//    val startedAt = System.nanoTime()
//    val queryParam = QueryParam(LabelWithDirection(1, 0))
//
//    val getRequests = for {
//      id <- (0 until 1000)
//    } yield {
//        new GetRequest("a".getBytes, Bytes.toBytes(id))
//      }
//
//    val rets = for {
//      i <- (0 until getRequests.size)
//      x = queryParam.toCacheKeyStr(getRequests(i))
//    } yield {
//        val rets = for {
//          j <- (0 until getRequests.size) if i != j
//          y = queryParam.toCacheKeyStr(getRequests(j))
//        } yield {
//            //          println(x, y)
//            x != y
//          }
//
//        rets.forall(identity)
//      }
//    println("\n\n")
//    val sames = getRequests.zip(getRequests).map { case (x, y) =>
//      val xHash = queryParam.toCacheKeyStr(x)
//      val yHash = queryParam.toCacheKeyStr(y)
////      println(xHash, yHash)
//      xHash == yHash
//    }.forall(identity)
//    val diffs = rets.forall(identity)
//    val duration = System.nanoTime() - startedAt
//
//    println(s">> strss: $duration")
//    sames && diffs
//  }
//
//  test("QueryParam toCacheKey bytes3") {
//    val startedAt = System.nanoTime()
//    var queryParam = QueryParam(LabelWithDirection(1, 0))
//
//    val getRequests = for {
//      id <- (0 until 1000)
//    } yield {
//        new GetRequest("a".getBytes, Bytes.toBytes(id))
//      }
//    val diff = getRequests.zip(getRequests).map { case (x, y) =>
//      queryParam.limit(0, 10)
//      var xHash = queryParam.toCacheKey(x)
//      queryParam.limit(1, 10)
//      var yHash = queryParam.toCacheKey(y)
//      //      println(xHash, yHash)
//      val ret = xHash != yHash
//
//      queryParam.limit(0, 10)
//      xHash = queryParam.toCacheKey(x)
//      queryParam.limit(0, 11)
//      yHash = queryParam.toCacheKey(y)
//
//      val ret2 = xHash != yHash
//      ret && ret2
//    }.forall(identity)
//
//    val duration = System.nanoTime() - startedAt
//
//    println(s">> diff: $duration")
//    diff
//  }

}
