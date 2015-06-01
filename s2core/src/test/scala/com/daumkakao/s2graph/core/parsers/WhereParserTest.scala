package com.daumkakao.s2graph.core.parsers

import com.daumkakao.s2graph.core.models.{HLabelMeta, HLabel}
import com.daumkakao.s2graph.core.types.{InnerValWithTs, LabelWithDirection, InnerVal, CompositeId}
import com.daumkakao.s2graph.core._
import org.scalatest.{Matchers, FunSuite}
import play.api.libs.json.Json

/**
 * Created by shon on 5/30/15.
 */
class WhereParserTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {
  // dummy data for dummy edge
  val srcId = CompositeId(0, InnerVal.withLong(1), isEdge = true, useHash = true)
  val tgtId = CompositeId(0, InnerVal.withLong(2), isEdge = true, useHash = true)
  val srcIdStr = CompositeId(0, InnerVal.withStr("abc"), isEdge = true, useHash = true)
  val tgtIdStr = CompositeId(0, InnerVal.withStr("def"), isEdge = true, useHash = true)
  val srcVertex = Vertex(srcId, ts)
  val tgtVertex = Vertex(tgtId, ts)
  val srcVertexStr = Vertex(srcIdStr, ts)
  val tgtVertexStr = Vertex(tgtIdStr, ts)


  def validate(labelName: String)(edge: Edge)(sql: String)(expected: Boolean) = {

    val checkedOpt = for (label <- HLabel.findByName(labelName)) yield {
      val labelMetas = HLabelMeta.findAllByLabelId(label.id.get, useCache = false)
      val metaMap = labelMetas.map { m => m.name -> m.seq } toMap
      val whereOpt = WhereParser(label).parse(sql)
      //      val props = Json.obj("is_hidden" -> true, "is_blocked" -> false)
      //      val propsInner = Management.toProps(label, props).map { case (k, v) => k -> InnerValWithTs.withInnerVal(v, ts) }.toMap
      //      val edge = Edge(srcVertex, srcVertex, labelWithDir, 0.toByte, System.currentTimeMillis(), 0, propsInner)
      whereOpt.isDefined && whereOpt.get.filter(edge)
    }
    checkedOpt.isDefined && checkedOpt.get == expected
  }

  test("check where clause not nested") {
    //    val labelOpt = HLabel.findByName(labelName)
    //    val label = labelOpt.get
    //    val labelWithDir = LabelWithDirection(label.id.get, 0)

    val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
    val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs(v, ts) }.toMap
    val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)
    play.api.Logger.debug(s"$edge")

    val f = validate(labelName)(edge) _

    val rets = List(
      f("is_hidden = false")(false),
      f("is_hidden != false")(true),
      f("is_hidden = true and is_blocked = true")(false),
      f("is_hidden = true and is_blocked = false")(true),
      f("time in (1, 2, 3) and is_blocked = true")(false),
      f("time in (1, 2, 3) or is_blocked = true")(true),
      f("time in (1, 2, 3) and is_blocked = false")(true),
      f("time in (1, 2, 4) and is_blocked = false")(false),
      f("time in (1, 2, 4) or is_blocked = false")(true),
      f("time not in (1, 2, 4)")(true),
      f("time in (1, 2, 3) and weight between 10 and 20 and is_blocked = false")(true),
      f("time in (1, 2, 4) or weight between 10 and 20 or is_blocked = true")(true)
    )
    println(rets)
    rets.forall(x => x)
  }
  test("check where clause nested") {
    //    val labelOpt = Label.findByName(testLabelName)
    //            labelOpt must beSome[Label]
    //            val label = labelOpt.get
    //            val labelWithDir = LabelWithDirection(label.id.get, 0)
    val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
    val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs(v, ts) }.toMap
    val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)

    val f = validate(labelName)(edge) _

    val rets = List(
      f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = false")(false),
      f("(time in (1, 2, 3) or is_blocked = true) or is_hidden = false")(true),
      f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = true")(true),
      f("(time in (1, 2, 3) or is_blocked = true) and is_hidden = true")(true),

      f("(time in (1, 2, 3) and weight between 1 and 10) or is_hidden = false")(true),
      f("(time in (1, 2, 4) or weight between 1 and 9) or is_hidden = false")(false),
      f("(time in (1, 2, 4) or weight between 1 and 9) or is_hidden = true")(true),
      f("(time in (1, 2, 3) or weight between 1 and 10) and is_hidden = false")(false)
    )
  }
  test("check where clause with from/to long") {
    val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
    val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs(v, ts) }.toMap
    val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)

    val f = validate(labelName)(edge) _

    //        f("from = abc")(false)
    f("_from = 2 or _to = 2")(true)
    f("_from = 1 and _to = 2")(true)
    f("_from = 3 and _to = 4")(false)
    f("_from = 0")(false)
  }

}
