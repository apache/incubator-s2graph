package com.kakao.s2graph.core

import com.kakao.s2graph.core.types.HBaseType
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

/**
 * Created by shon on 7/31/15.
 */
class QueryParamTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {
  val version = HBaseType.VERSION2
  val testEdge = Management.toEdge(ts, "insert", "1", "10", labelNameV2, "out", Json.obj("is_blocked" -> true, "phone_number" -> "xxxx", "age" -> 20).toString)
  test("EdgeTransformer toInnerValOpt") {

    /** only labelNameV2 has string type output */
    val jsVal = Json.arr(Json.arr("_to"), Json.arr("phone_number.$", "phone_number"), Json.arr("age.$", "age"))
    val transformer = EdgeTransformer(queryParamV2, jsVal)
    val convertedLs = transformer.transform(testEdge, None)

    convertedLs(0).tgtVertex.innerId.toString == "10" shouldBe true
    convertedLs(1).tgtVertex.innerId.toString == "phone_number.xxxx" shouldBe true
    convertedLs(2).tgtVertex.innerId.toString == "age.20" shouldBe true
    true
  }

}
