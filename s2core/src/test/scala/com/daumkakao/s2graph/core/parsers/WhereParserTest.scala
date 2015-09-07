package com.daumkakao.s2graph.core.parsers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.types._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

/**
 * Created by shon on 5/30/15.
 */
class WhereParserTest extends FunSuite with Matchers with TestCommonWithModels {
  // dummy data for dummy edge

  import HBaseType.{VERSION1, VERSION2}

  val ts = System.currentTimeMillis()

  def ids(version: String) = {
    val colId = if (version == VERSION2) columnV2.id.get else column.id.get
    val srcId = SourceVertexId(colId, InnerVal.withLong(1, version))
    val tgtId = TargetVertexId(colId, InnerVal.withLong(2, version))

    val srcIdStr = SourceVertexId(colId, InnerVal.withStr("abc", version))
    val tgtIdStr = TargetVertexId(colId, InnerVal.withStr("def", version))

    val srcVertex = Vertex(srcId, ts)
    val tgtVertex = Vertex(tgtId, ts)
    val srcVertexStr = Vertex(srcIdStr, ts)
    val tgtVertexStr = Vertex(tgtIdStr, ts)
    (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, version)
  }

  def validate(labelName: String)(edge: Edge)(sql: String)(expected: Boolean) = {
    for (label <- LABEL.findByName(labelName)) {
      val labelMetas = LABEMETA.findAllByLabelId(label.id.get, useCache = false)
      val metaMap = labelMetas.map { m => m.name -> m.seq } toMap
      val whereOpt = WhereParser(label).parse(sql)
      whereOpt.isSuccess shouldBe true

      println("=================================================================")
      println(sql)
      println(whereOpt.get)

      whereOpt.get.filter(edge) shouldBe expected
    }
  }

  test("check where clause not nested") {
    for {
      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- List(ids(VERSION1), ids(VERSION2))
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap
      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)
      val f = validate(labelName)(edge) _

      /** labelName label is long-long relation */
      f(s"_to=${tgtVertex.innerId.toString}")(true)

      // currently this throw exception since label`s _to is long type.
      f(s"_to=19230495")(false)
      f(s"_to!=19230495")(true)
    }
  }

  test("check where clause nested") {
    for {
      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- List(ids(VERSION1), ids(VERSION2))
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap
      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)

      val f = validate(labelName)(edge) _

      // time == 3
      f("time >= 3")(true)
      f("time > 2")(true)
      f("time <= 3")(true)
      f("time < 2")(false)

      f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = false")(false)
      f("(time in (1, 2, 3) or is_blocked = true) or is_hidden = false")(true)
      f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = true")(true)
      f("(time in (1, 2, 3) or is_blocked = true) and is_hidden = true")(true)

      f("((time in (  1, 2, 3) and weight between 1 and 10) or is_hidden=false)")(true)
      f("(time in (1, 2, 4 ) or weight between 1 and 9) or (is_hidden = false)")(false)
      f("(time in ( 1,2,4 ) or weight between 1 and 9) or is_hidden= true")(true)
      f("(time in (1,2,3) or weight between 1 and 10) and is_hidden =false")(false)
    }
  }

  test("check where clause with from/to long") {
    for {
      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- List(ids(VERSION1), ids(VERSION2))
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap
      val labelWithDirection = if (schemaVer == VERSION2) labelWithDirV2 else labelWithDir
      val edge = Edge(srcVertex, tgtVertex, labelWithDirection, 0.toByte, ts, 0, propsInner)
      val lname = if (schemaVer == VERSION2) labelNameV2 else labelName
      val f = validate(lname)(edge) _

      f(s"_from = -1 or _to = ${tgtVertex.innerId.value}")(true)
      f(s"_from = ${srcVertex.innerId.value} and _to = ${tgtVertex.innerId.value}")(true)
      f(s"_from = ${tgtVertex.innerId.value} and _to = 102934")(false)
      f(s"_from = -1")(false)
      f(s"_from in (-1, -0.1)")(false)
    }
  }

  test("time decay") {
    val ts = System.currentTimeMillis()

    for {
      i <- (0 until 10)
    } {
      val timeUnit = 60 * 60
      val diff = i * timeUnit
      val x = TimeDecay(1.0, 0.05, timeUnit)
      println(x.decay(diff))
    }
  }
}
