package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.types.EdgeType._
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class EdgeTypeTest extends FunSuite with Matchers  {

  val columnId = 0
  val labelId = 1
  val dir = 0.toByte
  val op = 0.toByte
  val labelOrderSeq = 1.toByte
  val labelWithDir = LabelWithDirection(labelId, dir)
  val ts = System.currentTimeMillis()
  val intVals = {
    val vals = (Int.MinValue until Int.MinValue + 10) ++
      (-128 to 128) ++ (Int.MaxValue - 10 until Int.MaxValue)
    vals.map { v => InnerVal(BigDecimal(v)) }
  }
  val idxPropsLs = Seq(
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("a"))),
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("ab"))),
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("b"))),
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(11)), (2.toByte -> InnerVal.withStr("a"))),
    Seq((0.toByte -> InnerVal.withLong(ts + 1)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("a")))
  )
  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0

  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0

  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0

  /** define functions for each HBaseType constructor and apply function */
  val edgeRowKeyCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
      EdgeRowKey(CompositeId(columnId, innerVal, isEdge = true, useHash = true),
        labelWithDir, labelOrderSeq, isInverted = false)
  val edgeRowKeyFromBytesFunc = (bytes: Array[Byte]) => EdgeRowKey(bytes, 0)

  val edgeQualifierCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
      EdgeQualifier(idxProps, CompositeId(columnId, innerVal, isEdge = true, useHash = false), op)
  val edgeQualifierFromBytesFunc = (bytes: Array[Byte]) => EdgeQualifier(bytes, 0, bytes.length)

  val edgeQualifierInvertedCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
    EdgeQualifierInverted(CompositeId(columnId, innerVal, isEdge = true, useHash = false))
  val edgeQualifierInvertedFromBytesFunc = (bytes: Array[Byte]) => EdgeQualifierInverted(bytes, 0)

  val edgeValueCreateFunc = (props: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
    EdgeValue(props)
  val edgeValueFromBytesFunc = (bytes: Array[Byte]) => EdgeValue(bytes, 0)


  def testOrder(idxPropsLs: Seq[Seq[(Byte, InnerVal)]], innerVals: Iterable[InnerVal])
                                       (createFunc: (Seq[(Byte, InnerVal)], InnerVal) => HBaseType,
                                        fromBytesFunc: Array[Byte] => HBaseType) = {
    /** check if increasing target vertex id is ordered properly with same indexProps */
    val rets = for {
      idxProps <- idxPropsLs
    } yield {
        val head = createFunc(idxProps, innerVals.head)
        val start = head
        var prev = head
        val rets = for {
          innerVal <- innerVals.tail
        } yield {
            val current = createFunc(idxProps, innerVal)
            val bytes = current.bytes
            val decoded = fromBytesFunc(bytes)
            println(s"$current vs $prev")
            val comp = largerThan(bytes, prev.bytes) &&
              largerThan(bytes, start.bytes) &&
              current == decoded

            prev = current
            comp
          }

        rets.forall(x => x)
      }

    rets.forall(x => x)
  }
  def testOrderReverse(idxPropsLs: Seq[Seq[(Byte, InnerVal)]], innerVals: Iterable[InnerVal])
               (createFunc: (Seq[(Byte, InnerVal)], InnerVal) => HBaseType,
                fromBytesFunc: Array[Byte] => HBaseType) = {
    /** check if increasing target vertex id is ordered properly with same indexProps */
    val rets = for {
      innerVal <- innerVals
    } yield {
        val head = createFunc(idxPropsLs.head, innerVal)
        val start = head
        var prev = head
        val rets = for {
          idxProps <- idxPropsLs.tail
        } yield {
            val current = createFunc(idxProps, innerVal)
            val bytes = current.bytes
            val decoded = fromBytesFunc(bytes)
            println(s"$current vs $prev")
            val comp = largerThan(bytes, prev.bytes) &&
              largerThan(bytes, start.bytes) &&
              current == decoded

            prev = current
            comp
          }

        rets.forall(x => x)
      }

    rets.forall(x => x)
  }
  test("test edge row key order with int source vertex id") {
    testOrder(idxPropsLs, intVals)(edgeRowKeyCreateFunc, edgeRowKeyFromBytesFunc) &&
    testOrderReverse(idxPropsLs, intVals)(edgeRowKeyCreateFunc, edgeRowKeyFromBytesFunc)
  }

  test("test edge row qualifier with int target vertex id") {
    testOrder(idxPropsLs, intVals)(edgeQualifierCreateFunc, edgeQualifierFromBytesFunc) &&
    testOrderReverse(idxPropsLs, intVals)(edgeQualifierCreateFunc, edgeQualifierFromBytesFunc)
  }

  test("test edge row qualifier inverted with int target vertex id") {
    testOrder(idxPropsLs, intVals)(edgeQualifierInvertedCreateFunc, edgeQualifierInvertedFromBytesFunc) &&
      testOrderReverse(idxPropsLs, intVals)(edgeQualifierInvertedCreateFunc, edgeQualifierInvertedFromBytesFunc)
  }

}
