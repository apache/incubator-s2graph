package test.controllers

import models._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.HBaseElement._
import com.daumkakao.s2graph.core.Management._
import org.specs2.mutable.Specification
import play.api.test.FakeApplication
import play.api.test.Helpers._
import play.api.libs.json.Json
import play.api.libs.json.JsObject


class RequestParserSpec extends Specification {

  // dummy data for dummy edge
  val testLabelName = "s2graph_label_test"
  val ts = System.currentTimeMillis()
  val srcId = CompositeId(0, InnerVal.withLong(1), isEdge = true, useHash = true)
  val tgtId = CompositeId(0, InnerVal.withLong(2), isEdge = true, useHash = true)
  val srcIdStr = CompositeId(0, InnerVal.withStr("abc"), isEdge = true, useHash = true)
  val tgtIdStr = CompositeId(0, InnerVal.withStr("def"), isEdge = true, useHash = true)
  val srcVertex = Vertex(srcId, ts)
  val tgtVertex = Vertex(tgtId, ts)
  val srcVertexStr = Vertex(srcIdStr, ts)
  val tgtVertexStr = Vertex(tgtIdStr, ts)
  

  def validate(labelName: String)(edge: Edge)(sql: String)(expected: Boolean) = {

    val checkedOpt = for (label <- Label.findByName(labelName)) yield {
      val labelMetas = LabelMeta.findAllByLabelId(label.id.get, useCache = false)
      val metaMap = labelMetas.map { m => m.name -> m.seq } toMap
      val whereOpt = WhereParser(label).parse(sql)
      whereOpt must beSome
      play.api.Logger.debug(whereOpt.toString)

      //      val props = Json.obj("is_hidden" -> true, "is_blocked" -> false)
      //      val propsInner = Management.toProps(label, props).map { case (k, v) => k -> InnerValWithTs.withInnerVal(v, ts) }.toMap
      //      val edge = Edge(srcVertex, srcVertex, labelWithDir, 0.toByte, System.currentTimeMillis(), 0, propsInner)
      val where = whereOpt.get
      where.filter(edge)
    }
    checkedOpt must beSome
    val checked = checkedOpt.get
    checked must beEqualTo(expected)
  }

  "RequestParser WhereParser" should {
    "check where clause not nested" in {
      running(FakeApplication()) {
        val labelOpt = Label.findByName(testLabelName)
        labelOpt must beSome[Label]
        val label = labelOpt.get
        val labelWithDir = LabelWithDirection(label.id.get, 0)

        val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
        val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs.withInnerVal(v, ts) }.toMap
        val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)
        play.api.Logger.debug(s"$edge")
        
        val f = validate(testLabelName)(edge)_

        f("is_hidden = false")(false)
        f("is_hidden != false")(true)
        f("is_hidden = true and is_blocked = true")(false)
        f("is_hidden = true and is_blocked = false")(true)
        f("time in (1, 2, 3) and is_blocked = true")(false)
        f("time in (1, 2, 3) or is_blocked = true")(true)
        f("time in (1, 2, 3) and is_blocked = false")(true)
        f("time in (1, 2, 4) and is_blocked = false")(false)
        f("time in (1, 2, 4) or is_blocked = false")(true)
        f("time not in (1, 2, 4)")(true)
        f("time in (1, 2, 3) and weight between 10 and 20 and is_blocked = false")(true)
        f("time in (1, 2, 4) or weight between 10 and 20 or is_blocked = true")(true)
      }
    }
    "check where clause nested" in {
      running(FakeApplication()) {
        val labelOpt = Label.findByName(testLabelName)
        labelOpt must beSome[Label]
        val label = labelOpt.get
        val labelWithDir = LabelWithDirection(label.id.get, 0)
        val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
        val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs.withInnerVal(v, ts) }.toMap
        val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)

        val f = validate(testLabelName)(edge)_

        f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = false")(false)
        f("(time in (1, 2, 3) or is_blocked = true) or is_hidden = false")(true)
        f("(time in (1, 2, 3) and is_blocked = true) or is_hidden = true")(true)
        f("(time in (1, 2, 3) or is_blocked = true) and is_hidden = true")(true)

        f("(time in (1, 2, 3) and weight between 1 and 10) or is_hidden = false")(true)
        f("(time in (1, 2, 4) or weight between 1 and 9) or is_hidden = false")(false)
        f("(time in (1, 2, 4) or weight between 1 and 9) or is_hidden = true")(true)
        f("(time in (1, 2, 3) or weight between 1 and 10) and is_hidden = false")(false)

//        f("(name in (a, abc, c) and weight between 1 and 10) or is_hidden = false")(true)
//        f("name between a and b or is_hidden = false")(true)
//        f("name = abc and is_hidden = true")(true)

      }
    }
    "check where clause with from/to long" in {
      running(FakeApplication()) {
        val labelOpt = Label.findByName(testLabelName)
        labelOpt must beSome[Label]
        val label = labelOpt.get
        val labelWithDir = LabelWithDirection(label.id.get, 0)
        val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
        val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs.withInnerVal(v, ts) }.toMap
        val edge = Edge(srcVertex, tgtVertex, labelWithDir, 0.toByte, ts, 0, propsInner)

        val f = validate(testLabelName)(edge)_

//        f("from = abc")(false)
        f("_from = 2 or _to = 2")(true)
        f("_from = 1 and _to = 2")(true)
        f("_from = 3 and _to = 4")(false)
        f("_from = 0")(false)

      }
    }
//    "check where clause with from/to string" in {
//      running(FakeApplication()) {
//
//        val labelOpt = Label.findByName("graph_test")
//        labelOpt must beSome[Label]
//        val label = labelOpt.get
//        val labelWithDir = LabelWithDirection(label.id.get, 0)
//        val propsInner = Management.toProps(label, js).map { case (k, v) => k -> InnerValWithTs.withInnerVal(v, ts) }.toMap
//        val edge = Edge(srcVertexStr, tgtVertexStr, labelWithDir, 0.toByte, ts, 0, propsInner)
//
//        val f = validate("graph_test")(edge)_
//
//        f("from = 2 or to = 2")(true)
//        f("from = a")(false)
//        f("from = abc")(true)
//        f("from = a or to = b")(false)
//        f("from = abc and to = def")(true)
//
//      }
//    }
  }
}