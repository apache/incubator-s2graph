package com.kakao.s2graph.core.Integrate

import com.kakao.s2graph.core.Management
import com.kakao.s2graph.core.mysqls._
import play.api.libs.json.{JsValue, Json}

class QueryTest extends IntegrateCommon {

//  test("test Query") {
//
//  }
//
//  test("test returnTree") {
//    def queryParents(id: Long) = Json.parse(
//      s"""
//        {
//          "returnTree": true,
//          "srcVertices": [
//          { "serviceName": "${testServiceName}",
//            "columnName": "${testColumnName}",
//            "id": ${id}
//           }],
//          "steps": [
//          [ {
//              "label": "${testLabelName}",
//              "direction": "out",
//              "offset": 0,
//              "limit": 2
//            }
//          ],[{
//              "label": "${testLabelName}",
//              "direction": "in",
//              "offset": 0,
//              "limit": -1
//            }
//          ]]
//        }""".stripMargin)
//
//    val src = 100
//    val tgt = 200
//    val labelName = testLabelName
//
//    insertEdges(
//      toEdge(1001, "insert", "e", src, tgt, labelName)
//    )
//
//    val result = getEdges(queryParents(src))
//    val parents = (result \ "results").as[Seq[JsValue]]
//    val ret = parents.forall { edge => (edge \ "parents").as[Seq[JsValue]].size == 1 }
//
//    ret === true
//  }

  override def beforeAll(): Unit = {
    super.beforeAll()


  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
