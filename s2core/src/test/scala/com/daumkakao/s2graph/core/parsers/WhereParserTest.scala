package com.daumkakao.s2graph.core.parsers

import com.daumkakao.s2graph.core.models.{LabelMeta, Label}
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.types2.{InnerValLikeWithTs, InnerVal, CompositeId}
import org.scalatest.{Matchers, FunSuite}
import play.api.libs.json.Json

/**
 * Created by shon on 5/30/15.
 */
class WhereParserTest extends FunSuite with Matchers with TestCommonWithModels {
  // dummy data for dummy edge
  val ts = System.currentTimeMillis()
  val ids = for {
    version <- List("v1", "v2")
  } yield {
    val srcId = CompositeId(column.id.get, InnerVal.withLong(1, version), isEdge = true, useHash = true)
    val tgtId = CompositeId(column.id.get, InnerVal.withLong(2, version), isEdge = true, useHash = true)
    val srcIdStr = CompositeId(column.id.get, InnerVal.withStr("abc", version), isEdge = true, useHash = true)
    val tgtIdStr = CompositeId(column.id.get, InnerVal.withStr("def", version), isEdge = true, useHash = true)
    val srcVertex = Vertex(srcId, ts, schemaVersion = Some(version))
    val tgtVertex = Vertex(tgtId, ts, schemaVersion = Some(version))
    val srcVertexStr = Vertex(srcIdStr, ts, schemaVersion = Some(version))
    val tgtVertexStr = Vertex(tgtIdStr, ts, schemaVersion = Some(version))
    (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, version)
  }

  def validate(labelName: String)(edge: Edge)(sql: String)(expected: Boolean) = {

    val checkedOpt = for (label <- Label.findByName(labelName)) yield {
      val labelMetas = LabelMeta.findAllByLabelId(label.id.get, useCache = false)
      val metaMap = labelMetas.map { m => m.name -> m.seq } toMap
      val whereOpt = WhereParser(label).parse(sql)
      whereOpt.isDefined && whereOpt.get.filter(edge)
    }
    val ret = checkedOpt.isDefined && checkedOpt.get == expected
    println(s"${edge.schemaVer}, $labelName, $edge, $sql, $expected, $ret")
    ret
  }

//  test("check where clause not nested") {
//    for {
//      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- ids
//    } {
//      /** test for each version */
//      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
//      val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap
//      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner, Some(schemaVer))
//      play.api.Logger.debug(s"$edge")
//
//      val f = validate(labelName)(edge) _
//
//      val rets = List(
//        f("is_hidden = false")(false),
//        f("is_hidden != false")(true),
//        f("is_hidden = true and is_blocked = true")(false),
//        f("is_hidden = true and is_blocked = false")(true),
//        f("time in (1, 2, 3) and is_blocked = true")(false),
//        f("time in (1, 2, 3) or is_blocked = true")(true),
//        f("time in (1, 2, 3) and is_blocked = false")(true),
//        f("time in (1, 2, 4) and is_blocked = false")(false),
//        f("time in (1, 2, 4) or is_blocked = false")(true),
//        f("time not in (1, 2, 4)")(true),
//        f("time in (1, 2, 3)")(true),
//        f("weight between 10 and 20")(true),
//        f("time in (1, 2, 3) and weight between 10 and 20")(true),
//        f("time in (1, 2, 3) and weight between 10 and 20 and is_blocked = false")(true),
//        f("time in (1, 2, 4) or weight between 10 and 20 or is_blocked = true")(true)
//      )
//      println(edge.schemaVer, rets)
//      rets.forall(x => x) shouldBe true
//    }
//  }
//  test("check where clause nested") {
//    for {
//      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- ids
//    } {
//      /** test for each version */
//      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
//      val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap
//      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner, Some(schemaVer))
//
//      val f = validate(labelName)(edge) _
//
//      val rets = List(
//        f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = false")(false),
//        f("(time in (1, 2, 3) or is_blocked = true) or is_hidden = false")(true),
//        f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = true")(true),
//        f("(time in (1, 2, 3) or is_blocked = true) and is_hidden = true")(true),
//
//        f("(time in (1, 2, 3) and weight between 1 and 10) or is_hidden = false")(true),
//        f("(time in (1, 2, 4) or weight between 1 and 9) or is_hidden = false")(false),
//        f("(time in (1, 2, 4) or weight between 1 and 9) or is_hidden = true")(true),
//        f("(time in (1, 2, 3) or weight between 1 and 10) and is_hidden = false")(false)
//      )
//      println(edge.schemaVer, rets)
//      rets.forall(x => x) shouldBe true
//    }
//  }
  test("check where clause with from/to long") {
    for {
      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- ids
      if schemaVer == "v2"
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap
      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner, Some(schemaVer))

      val f = validate(labelName)(edge) _

      //        f("from = abc")(false)
      val rets = List(
        f(s"_from = -1 or _to = ${tgtVertex.innerId.value}")(true),
        f(s"_from = ${srcVertex.innerId.value} and _to = ${tgtVertex.innerId.value}")(true)
//        ,
//        f(s"_from = ${tgtVertex.innerId.value} and _to = 102934")(false),
//        f(s"_from = -1")(false),
//        f(s"_from in (-1, -0.1)")(false)
      )
      println(edge.schemaVer, rets)
      rets.forall(x => x) shouldBe true
    }
  }

}
