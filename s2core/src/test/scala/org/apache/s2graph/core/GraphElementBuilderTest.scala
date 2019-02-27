package org.apache.s2graph.core

import org.apache.s2graph.core.mysqls.Label
import org.scalatest._

class GraphElementBuilderTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {
  initTests()

  /*
    In Test CommonWithModels,
      - labelName: undirectedLabelName is created as undirected label.
      - labelName: labelName is created as directed label.
   */
  val timestamp = "1"
  val operation = "insert"
  val logType = "edge"
  val srcId = 1L
  val tgtId = 101L
  val props = "{}"
  val outDirection = "out"
  val inDirection = "in"


  test("toEdge with directed label. direction out.") {
    val parts = Array(
      timestamp, operation, logType, srcId.toString, tgtId.toString, labelName, props, outDirection
    )

    val s2EdgeLike = builder.toEdge(parts).get
    s2EdgeLike.srcVertex.id.innerId.value shouldBe(srcId)
    s2EdgeLike.tgtVertex.id.innerId.value shouldBe(tgtId)
    s2EdgeLike.getDirection() shouldBe("out")

  }
  test("toEdge with directed label. direction in.") {
    val parts = Array(
      timestamp, operation, logType, srcId.toString, tgtId.toString, labelName, props, inDirection
    )

    val s2EdgeLike = builder.toEdge(parts).get
    s2EdgeLike.srcVertex.id.innerId.value shouldBe(srcId)
    s2EdgeLike.tgtVertex.id.innerId.value shouldBe(tgtId)
    s2EdgeLike.getDirection() shouldBe("in")

  }
  test("toEdge with undirected label. direction out.") {
    // undirectedLabelName has tgtColumnType as string.
    val parts = Array(
      timestamp, operation, logType, srcId.toString, tgtId.toString, undirectedLabelName, props, outDirection
    )

    val s2EdgeLike = builder.toEdge(parts).get
    s2EdgeLike.srcVertex.id.innerId.value shouldBe(srcId)
    s2EdgeLike.tgtVertex.id.innerId.value shouldBe(tgtId.toString)
    s2EdgeLike.getDirection() shouldBe("out")
  }
}
