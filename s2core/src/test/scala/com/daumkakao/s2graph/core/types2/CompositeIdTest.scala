package com.daumkakao.s2graph.core.types2

import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 6/7/15.
 */
class CompositeIdTest extends FunSuite with Matchers {
  val v1 = "v1"
  val v2 = "v2"
  val longIds = (Long.MinValue until Long.MinValue + 10) ++
    (-1L to 1L) ++
    (Long.MaxValue - 9L to Long.MaxValue)


  val intIds =(Int.MinValue until Int.MinValue + 10) ++
    (-1 to 1) ++
    (Int.MaxValue - 9 to Int.MaxValue)


  test("test long ids version 1") {
    for {
      version <- List("v1", "v2")
    } yield {
      val v1Ids = longIds.map { id => InnerVal.withLong(id, version)}
      for {
        isEdge <- List(true, false)
        useHash <- List(true, false)
        id <- v1Ids
      } yield {
        val compositeId = CompositeId(0, id, isEdge, useHash)
        val decoded = CompositeId.fromBytes(compositeId.bytes, 0, isEdge, useHash, version)
        compositeId == decoded shouldBe true
      }
    }
  }
}
