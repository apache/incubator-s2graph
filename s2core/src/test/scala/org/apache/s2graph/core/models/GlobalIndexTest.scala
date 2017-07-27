package org.apache.s2graph.core.models

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.mysqls.GlobalIndex
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import scala.collection.JavaConversions._

class GlobalIndexTest extends FunSuite with Matchers with TestCommonWithModels with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    initTests()
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

  test("test buildGlobalIndex.") {
    management.buildGlobalIndex("test_global", Seq("weight", "date", "name"))
  }

  test("findGlobalIndex.") {
    // (weight: 34) AND (weight: [1 TO 100])
    val idx1 = management.buildGlobalIndex("test-1", Seq("weight", "age", "name"))
    val idx2 = management.buildGlobalIndex("test-2", Seq("gender", "age"))
    val idx3 = management.buildGlobalIndex("test-3", Seq("class"))

    var hasContainers = Seq(
      new HasContainer("weight", P.eq(Int.box(34))),
      new HasContainer("age", P.between(Int.box(1), Int.box(100)))
    )

    GlobalIndex.findGlobalIndex(hasContainers) shouldBe(Option(idx1))

    hasContainers = Seq(
      new HasContainer("gender", P.eq(Int.box(34))),
      new HasContainer("age", P.eq(Int.box(34))),
      new HasContainer("class", P.eq(Int.box(34)))
    )

    GlobalIndex.findGlobalIndex(hasContainers) shouldBe(Option(idx2))

    hasContainers = Seq(
      new HasContainer("class", P.eq(Int.box(34))),
      new HasContainer("_", P.eq(Int.box(34)))
    )

    GlobalIndex.findGlobalIndex(hasContainers) shouldBe(Option(idx3))

    hasContainers = Seq(
      new HasContainer("key", P.eq(Int.box(34))),
      new HasContainer("value", P.eq(Int.box(34)))
    )

    GlobalIndex.findGlobalIndex(hasContainers) shouldBe(None)

  }
}
