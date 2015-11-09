package com.kakao.s2graph.core

import com.kakao.s2graph.core.types.LabelWithDirection
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.GetRequest
import org.scalatest.{FunSuite, Matchers}

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

  test("QueryParam toCacheKey bytes") {
    val startedAt = System.nanoTime()
    val queryParam = QueryParam(LabelWithDirection(1, 0))

    val getRequests = {
      for {
        id <- 0 until 1000
      } yield {
        new GetRequest("a".getBytes, Bytes.toBytes(id))
      }
    }

    for {
      i <- getRequests.indices
      x = queryParam.toCacheKey(getRequests(i))
    } {
      for {
        j <- getRequests.indices if i != j
        y = queryParam.toCacheKey(getRequests(j))
      } {
        x should not equal y
      }
    }

    getRequests.zip(getRequests).foreach { case (x, y) =>
      val xHash = queryParam.toCacheKey(x)
      val yHash = queryParam.toCacheKey(y)
//      println(xHash, yHash)
      xHash should be(yHash)
    }
    val duration = System.nanoTime() - startedAt

    println(s">> bytes: $duration")
  }

  test("QueryParam toCacheKey string") {
    val startedAt = System.nanoTime()
    val queryParam = QueryParam(LabelWithDirection(1, 0))

    val getRequests = {
      for {
        id <- 0 until 1000
      } yield {
        new GetRequest("a".getBytes, Bytes.toBytes(id))
      }
    }

    for {
      i <- getRequests.indices
      x = queryParam.toCacheKeyStr(getRequests(i))
    } {
      for {
        j <- getRequests.indices if i != j
        y = queryParam.toCacheKeyStr(getRequests(j))
      } yield {
        //          println(x, y)
        x should not equal y
      }
    }

    getRequests.zip(getRequests).foreach { case (x, y) =>
      val xHash = queryParam.toCacheKeyStr(x)
      val yHash = queryParam.toCacheKeyStr(y)
//      println(xHash, yHash)
      xHash should equal(yHash)
    }
    val duration = System.nanoTime() - startedAt

    println(s">> string: $duration")
  }

  test("QueryParam toCacheKey bytes3") {
    val startedAt = System.nanoTime()
    val queryParam = QueryParam(LabelWithDirection(1, 0))

    val getRequests = {
      for {
        id <- 0 until 1000
      } yield {
        new GetRequest("a".getBytes, Bytes.toBytes(id))
      }
    }

    getRequests.zip(getRequests).foreach { case (x, y) =>
      x shouldBe y
      queryParam.limit(0, 10)
      var xHash = queryParam.toCacheKey(x)
      xHash shouldBe queryParam.toCacheKey(y)
      queryParam.limit(1, 10)
      var yHash = queryParam.toCacheKey(y)
      queryParam.toCacheKey(x) shouldBe yHash
//      println(xHash, yHash)
      xHash should not be yHash

      queryParam.limit(0, 10)
      xHash = queryParam.toCacheKey(x)
      queryParam.limit(0, 11)
      yHash = queryParam.toCacheKey(y)

      xHash should not be yHash
    }

    val duration = System.nanoTime() - startedAt

    println(s">> diff: $duration")
  }

}
