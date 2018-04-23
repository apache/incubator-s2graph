/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.parsers

import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{ServiceColumn, Label, LabelMeta}
import org.apache.s2graph.core.rest.TemplateHelper
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

import scala.util.{Random, Try}

class WhereParserTest extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

  import HBaseType._

  val ts = System.currentTimeMillis()
  val dummyTs = LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion)

  def validate(label: Label)(edge: S2EdgeLike)(sql: String)(expected: Boolean) = {
    def debug(whereOpt: Try[Where]) = {
      println("==================")
      println(s"$whereOpt")
      println(s"$edge")
      println("==================")
    }

    val whereOpt = WhereParser().parse(sql)
    if (whereOpt.isFailure) {
      debug(whereOpt)
      whereOpt.get // touch exception
    } else {
      val ret = whereOpt.get.filter(edge)
      if (ret != expected) {
        debug(whereOpt)
      }

      ret shouldBe expected
    }
  }

  def ids = for {
    version <- ValidVersions
  } yield {
    val srcId = SourceVertexId(ServiceColumn.Default, InnerVal.withLong(1, version))
    val tgtId =
      if (version == VERSION2) TargetVertexId(ServiceColumn.Default, InnerVal.withStr("2", version))
      else TargetVertexId(ServiceColumn.Default, InnerVal.withLong(2, version))

    val srcVertex = builder.newVertex(srcId, ts)
    val tgtVertex = builder.newVertex(tgtId, ts)
    val (_label, dir) = if (version == VERSION2) (labelV2, labelWithDirV2.dir) else (label, labelWithDir.dir)

    (srcVertex, tgtVertex, _label, dir)
  }

  test("check where clause not nested") {
    for {
      (srcVertex, tgtVertex, label, dir) <- ids
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "phone_number" -> "1234")
      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val edge = builder.newEdge(srcVertex, tgtVertex, label, dir, 0.toByte, ts, propsInner)

      val f = validate(label)(edge) _

      /** labelName label is long-long relation */
      f(s"_to=${tgtVertex.innerId}")(true)
      f(s"_to=19230495")(false)
      f(s"_to!=19230495")(true)
      f(s"phone_number=1234")(true)
    }
  }

  test("check where clause with string literal") {
    for {
      (srcVertex, tgtVertex,
      label, dir) <- ids
    } {
      /** test for each version */
      var js = Json.obj("phone_number" -> "")
      var propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      var edge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, propsInner)

      var f = validate(label)(edge) _
      f(s"phone_number = '' ")(true)

      js = Json.obj("phone_number" -> "010 3167 1897")
      propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      edge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, propsInner)

      f = validate(label)(edge) _
      f(s"phone_number = '010 3167 1897' ")(true)

      js = Json.obj("phone_number" -> "010' 3167 1897")
      propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      edge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, propsInner)

      f = validate(label)(edge) _
      f(s"phone_number = '010\\' 3167 1897' ")(true)
    }
  }

  test("check where clause nested") {
    for {
      (srcVertex, tgtVertex, label, dir) <- ids
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val edge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, propsInner)

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
      (srcVertex, tgtVertex, label, dir) <- ids
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 3, "name" -> "abc")
      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val edge = builder.newEdge(srcVertex, tgtVertex, label, dir, 0.toByte, ts, propsInner)

      val f = validate(label)(edge) _

      f(s"_from = -1 or _to = ${tgtVertex.innerId.value}")(true)
      f(s"_to = 2")(true)
      f(s"_from = ${srcVertex.innerId.value} and _to = ${tgtVertex.innerId.value}")(true)
      f(s"_from = ${tgtVertex.innerId.value} and _to = 102934")(false)
      f(s"_from = -1")(false)
      f(s"_from in (-1, -0.1)")(false)
    }
  }

  test("check where clause with parent") {
    for {
      (srcVertex, tgtVertex, label, dir) <- ids
    } {
      /** test for each version */
      val js = Json.obj("is_hidden" -> true, "is_blocked" -> false, "weight" -> 10, "time" -> 1, "name" -> "abc")
      val parentJs = Json.obj("is_hidden" -> false, "is_blocked" -> false, "weight" -> 20, "time" -> 3, "name" -> "a")

      val propsInner = Management.toProps(label, js.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs
      val parentPropsInner = Management.toProps(label, parentJs.fields).map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }.toMap + dummyTs

      val grandParentEdge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, parentPropsInner)

      val parentEdge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, parentPropsInner,
        parentEdges = Seq(EdgeWithScore(grandParentEdge, 1.0, grandParentEdge.innerLabel)))

      val edge = builder.newEdge(srcVertex, tgtVertex, label, labelWithDir.dir, 0.toByte, ts, propsInner,
        parentEdges = Seq(EdgeWithScore(parentEdge, 1.0, grandParentEdge.innerLabel)))
      
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

  test("replace reserved") {
    val ts = 0
    import TemplateHelper._

    calculate(ts, 1, "minute") should be(minute + ts)
    calculate(ts, 1, "hour") should be(hour + ts)
    calculate(ts, 1, "day") should be(day + ts)

    calculate(ts + 10, 1, "HOUR") should be(hour + ts + 10)
    calculate(ts + 10, 1, "DAY") should be(day + ts + 10)

    val body =
      """{
        	"minute": ${1 minute},
        	"day": ${1day},
          "hour": ${1hour},
          "-day": "${-10 day}",
          "-hour": ${-10 hour},
          "now": "${now}"
        }
      """

    val parsed = replaceVariable(ts, body)
    val json = Json.parse(parsed)

    (json \ "minute").as[Long] should be(1 * minute + ts)

    (json \ "day").as[Long] should be(1 * day + ts)
    (json \ "hour").as[Long] should be(1 * hour + ts)

    (json \ "-day").as[Long] should be(-10 * day + ts)
    (json \ "-hour").as[Long] should be(-10 * hour + ts)

    (json \ "now").as[Long] should be(ts)

    val otherBody =
      """{
          "nextminute": "${next_minute}",
          "nextday": "${next_day}",
          "3dayago": "${next_day - 3 day}",
          "nexthour": "${next_hour}"
        }"""

    val currentTs = 1474422964000l
    val expectedMinuteTs = currentTs / minute * minute + minute
    val expectedDayTs = currentTs / day * day + day
    val expectedHourTs = currentTs / hour * hour + hour
    val threeDayAgo = expectedDayTs - 3 * day
    val currentTsLs = (1 until 1000).map(currentTs + _)

    currentTsLs.foreach { ts =>
      val parsed = replaceVariable(ts, otherBody)
      val json = Json.parse(parsed)

      (json \ "nextminute").as[Long] should be(expectedMinuteTs)
      (json \ "nextday").as[Long] should be(expectedDayTs)
      (json \ "nexthour").as[Long] should be(expectedHourTs)
      (json \ "3dayago").as[Long] should be(threeDayAgo)
    }

    (0 until 1000).forall { ith =>
      val r = replaceVariable(ts, "${randint( 10,  30 )}").toInt
      r >= 10 && r < 30
    }
  }

  test("check parse contains") {
    val dummyLabel = ids.head._3

    val sql = "name = 'daewon'"
    val whereOpt = WhereParser().parse(sql)
    println(whereOpt)
  }

}
