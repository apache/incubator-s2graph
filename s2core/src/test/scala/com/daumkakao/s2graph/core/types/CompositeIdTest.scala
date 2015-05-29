package com.daumkakao.s2graph.core.types

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class CompositeIdTest extends FunSuite with Matchers {

  val columnId = 0
  /** these constants need to be sorted asc order for test to run */
  val strings = List(
    "abc", "abd", "ac", "aca", "b"
  ).map(InnerVal(_))

  val nums = {
    val decimals = (Long.MinValue until Long.MinValue + 10).map(BigDecimal(_)) ++
      (Int.MinValue until Int.MinValue + 10).map(BigDecimal(_)) ++
      (-9999.9f until -9999.1f by 0.1f).map(BigDecimal(_)) ++
      (-128 to 128).map(BigDecimal(_)) ++
      (129.0 until 130.0 by 0.1).map(BigDecimal(_)) ++
      (Int.MaxValue - 10 until Int.MaxValue).map(BigDecimal(_)) ++
      (Long.MaxValue - 10 until Long.MaxValue).map(BigDecimal(_))
    decimals.map(InnerVal(_))
  }

  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0

  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0

  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0

  def testOrder(innerVals: Iterable[InnerVal]) = {
    val rets = for {
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } yield {
        val head = CompositeId(columnId, innerVals.head, isEdge = isEdge, useHash = useHash)
        val start = head
        var prev = head

        val rets = for {
          innerVal <- innerVals.tail
        } yield {
            val current = CompositeId(columnId, innerVal, isEdge = isEdge, useHash = useHash)
            val decoded = CompositeId(current.bytes, 0, isEdge = isEdge, useHash = useHash)

            val comp = largerThan(current.bytes, prev.bytes) &&
            largerThan(current.bytes, start.bytes) &&
            current == decoded
            comp
          }
        rets.forall(x => x)
      }

    rets.forall(x => x)
  }

  test("order of compositeId numeric") {
    testOrder(nums)
  }
  test("order of compositeId strings") {
    testOrder(strings)
  }
}
