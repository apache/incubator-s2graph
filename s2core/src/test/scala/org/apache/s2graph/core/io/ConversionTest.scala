package org.apache.s2graph.core.io

//import org.apache.s2graph.core.{EdgeId, S2VertexPropertyId}
import org.apache.s2graph.core.mysqls.{ColumnMeta, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, VertexId}
import org.apache.s2graph.core.utils.logger
import org.scalatest.{FunSuite, Matchers}

//class ConversionsTest extends FunSuite with Matchers {
//  import Conversions._
//  import org.apache.s2graph.core.types.HBaseType._
//
//  test("innerVal test") {
//    val value = InnerVal.withStr("a,b,c", DEFAULT_VERSION)
//
//    val writer = InnerValLikeWrites
//    val reader = InnerValLikeReads
//    val json = writer.writes(value)
//    val deserialized = reader.reads(json).get
//
//    println(s"[Given]: $value")
//    println(s"[GivenJson]: $json")
//    println(s"[Deserialized]: $deserialized")
//    println(s"[DeserializedJson]: ${writer.writes(deserialized)}")
//
//    value shouldBe (deserialized)
//  }
//
//  test("serviceColumn test") {
//    val value = Service(Option(1), "serviceName", "accessToken", "cluster", "hTableName", 10, Option(10), None)
//    val writer = serviceWrites
//    val reader = serviceReads
//    val json = writer.writes(value)
//    val deserialized = reader.reads(json).get
//
//    println(s"[Given]: $value")
//    println(s"[GivenJson]: $json")
//    println(s"[Deserialized]: $deserialized")
//    println(s"[DeserializedJson]: ${writer.writes(deserialized)}")
//
//    value shouldBe (deserialized)
//
//  }
//
//  test("s2VertexPropertyId test") {
////    val column = ServiceColumn(Option(10), 1, "vertex", "string", DEFAULT_VERSION)
//    val columnMeta = ColumnMeta(Option(1), 1, "name", 1.toByte, "string")
//    val innerVal = InnerVal.withStr("shon", DEFAULT_VERSION)
//    val value = S2VertexPropertyId(columnMeta, innerVal)
//
//    val writer = s2VertexPropertyIdWrites
//    val reader = s2VertexPropertyIdReads
//    val json = writer.writes(value)
//    val deserialized = reader.reads(json).get
//
//    println(s"[Given]: $value")
//    println(s"[GivenJson]: $json")
//    println(s"[Deserialized]: $deserialized")
//    println(s"[DeserializedJson]: ${writer.writes(deserialized)}")
//
//    value shouldBe (deserialized)
//  }
//
//  test("s2VertexId test") {
//    val column = ServiceColumn(Option(10), 1, "vertex", "string", DEFAULT_VERSION)
//    val innerVal = InnerVal.withStr("vertex.1", DEFAULT_VERSION)
//    val value = VertexId(column, innerVal)
//
//    val writer = s2VertexIdWrites
//    val reader = s2VertexIdReads
//    val json = writer.writes(value)
//    val deserialized = reader.reads(json).get
//
//    println(s"[Given]: $value")
//    println(s"[GivenJson]: $json")
//    println(s"[Deserialized]: $deserialized")
//    println(s"[DeserializedJson]: ${writer.writes(deserialized)}")
//
//    value shouldBe (deserialized)
//  }
//
//  test("EdgeId test") {
//    val s =
//      s"""
//         |{
//         |	"srcVertexId": {
//         |		"value": 1,
//         |		"dataType": "long",
//         |		"schemaVersion": "v3"
//         |	},
//         |	"tgtVertexId": {
//         |		"value": 2,
//         |		"dataType": "bigDecimal",
//         |		"schemaVersion": "v3"
//         |	},
//         |	"labelName": "knows",
//         |	"direction": "out",
//         |	"ts": 0
//         |}
//       """.stripMargin
//    val value = EdgeId.fromString(s)
//
//    val writer = s2EdgeIdWrites
//    val reader = s2EdgeIdReads
//    val json = writer.writes(value)
//    val deserialized = reader.reads(json).get
//
//    println(s"[Given]: $value")
//    println(s"[GivenJson]: $json")
//    println(s"[Deserialized]: $deserialized")
//    println(s"[DeserializedJson]: ${writer.writes(deserialized)}")
//
//    value shouldBe (deserialized)
//  }
//}
