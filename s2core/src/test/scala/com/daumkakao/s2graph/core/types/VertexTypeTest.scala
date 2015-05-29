package com.daumkakao.s2graph.core.types

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class VertexTypeTest extends FunSuite with Matchers {


  import VertexType._

  val columnId = 1
  val intVals = {
    val vals = (Int.MinValue until Int.MinValue + 10) ++
      (-128 to 128) ++ (Int.MaxValue - 10 until Int.MaxValue)
    vals.map { v => InnerVal(BigDecimal(v)) }
  }
  val props = Seq(
    (1.toByte, InnerVal("abc")),
    (2.toByte, InnerVal(BigDecimal(37))),
    (3.toByte, InnerVal(BigDecimal(0.01f)))
  )

  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0

  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0

  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0

  def testVertexRowKeyOrder(innerVals: Iterable[InnerVal])= {
    val rets = for {
      isEdge <- List(false)
      useHash <- List(true)
    } yield {
        val head = VertexRowKey(CompositeId(columnId, innerVals.head, isEdge = isEdge, useHash = useHash))
        val start = head
        var prev = head

        val rets = for {
          innerVal <- innerVals.tail
        } yield {
            val current = VertexRowKey(CompositeId(columnId, innerVal, isEdge = isEdge, useHash = useHash))
            val decoded = VertexRowKey(current.bytes, 0)

            val comp = largerThan(current.bytes, prev.bytes) &&
              largerThan(current.bytes, start.bytes) &&
              current == decoded

            comp
          }
        rets.forall(x => x)
      }
    rets.forall(x => x)
  }

  test("test vertex row key order with int id type") {
    testVertexRowKeyOrder(intVals)
  }

}
