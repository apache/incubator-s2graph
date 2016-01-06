package com.kakao.s2graph.core.parsers

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Label, LabelMeta}
import com.kakao.s2graph.core.types._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

class WhereParserTest extends FunSuite with Matchers with TestCommonWithModels {
  // dummy data for dummy edge

  import HBaseType.{VERSION1, VERSION2}

  val ts = System.currentTimeMillis()
  val dummyTs = (LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion))

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

  val labelMap = Map(label.label -> label)

  def validate(label: Label)(edge: Edge)(sql: String)(expected: Boolean) = {
    val whereOpt = WhereParser(label).parse(sql)
    whereOpt.isSuccess shouldBe true

    println("=================================================================")
    println(sql)
    println(whereOpt.get)

    val ret = whereOpt.get.filter(edge)
    if (ret != expected) {
      println("==================")
      println(s"$whereOpt")
      println(s"$edge")
      println("==================")
    }
    ret shouldBe expected
  }

  test("check where clause not nested") {
    for {
      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- List(ids(VERSION1), ids(VERSION2))
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, propsInner)
      val f = validate(label)(edge) _

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
      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, propsInner)

      val f = validate(label)(edge) _

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
      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val labelWithDirection = if (schemaVer == VERSION2) labelWithDirV2 else labelWithDir
      val edge = Edge(srcVertex, tgtVertex, labelWithDirection, 0.toByte, ts, propsInner)
      val lname = if (schemaVer == VERSION2) labelNameV2 else labelName
      val f = validate(label)(edge) _

      f(s"_from = -1 or _to = ${tgtVertex.innerId.value}")(true)
      f(s"_from = ${srcVertex.innerId.value} and _to = ${tgtVertex.innerId.value}")(true)
      f(s"_from = ${tgtVertex.innerId.value} and _to = 102934")(false)
      f(s"_from = -1")(false)
      f(s"_from in (-1, -0.1)")(false)
    }
  }


  test("check where clause with parent") {
    for {
      (srcId, tgtId, srcIdStr, tgtIdStr, srcVertex, tgtVertex, srcVertexStr, tgtVertexStr, schemaVer) <- List(ids(VERSION1), ids(VERSION2))
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 1, "name" -> "abc")
      val parentJs = Json.obj("is_hidden" -> false, "is_blocked" -> false, "weight" -> 20, "time" -> 3, "name" -> "a")

      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val parentPropsInner = Management.toProps(label, parentJs.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs

      val grandParentEdge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, parentPropsInner)
      val parentEdge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, parentPropsInner,
        parentEdges = Seq(EdgeWithScore(grandParentEdge, 1.0)))
      val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, propsInner,
        parentEdges = Seq(EdgeWithScore(parentEdge, 1.0)))

      println(edge.toString)
      println(parentEdge.toString)
      println(grandParentEdge.toString)

      val f = validate(label)(edge) _

      // Compare edge's prop(`_from`) with edge's prop(`name`)
      f("_from = 1")(true)
      f("_to = 2")(true)
      f("_from = 123")(false)
      f("_from = time")(true)

      // Compare edge's prop(`weight`) with edge's prop(`time`)
      f("weight = time")(false)
      f("weight = is_blocked")(false)

      // Compare edge's prop(`weight`) with parent edge's prop(`weight`)
      f("_parent.is_blocked = is_blocked")(true)
      f("is_hidden = _parent.is_hidden")(false)
      f("_parent.weight = weight")(false)

      // Compare edge's prop(`is_hidden`) with parent of parent edge's prop(`is_hidden`)
      f("_parent._parent.is_hidden = is_hidden")(false)
      f("_parent._parent.is_blocked = is_blocked")(true)
      f("_parent._parent.weight = weight")(false)
      f("_parent._parent.weight = _parent.weight")(true)
    }
  }

  //  test("time decay") {
  //    val ts = System.currentTimeMillis()
  //
  //    for {
  //      i <- (0 until 10)
  //    } {
  //      val timeUnit = 60 * 60
  //      val diff = i * timeUnit
  //      val x = TimeDecay(1.0, 0.05, timeUnit)
  //      println(x.decay(diff))
  //    }
  //  }
}
