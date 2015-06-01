package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models._
import com.daumkakao.s2graph.core.types.{InnerValWithTs, HBaseType, InnerVal, LabelWithDirection}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{PutRequest, KeyValue}
import play.api.libs.json.{JsNumber, JsString, JsBoolean}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 6/1/15.
 */
trait TestCommon {



  val ts = System.currentTimeMillis()
  val testServiceId = 1
  val testColumnId = 1
  val testLabelId = 1
  val testDir = GraphUtil.directions("out")
  val testOp = GraphUtil.operations("insert")
  val testLabelOrderSeq = HLabelIndex.defaultSeq
  val testLabelWithDir = LabelWithDirection(testLabelId, testDir)



  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0

  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0

  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0

  /** */
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
  val idxPropsWithTsLs = idxPropsLs.map { idxProps =>
    idxProps.map { case (k, v) => k -> InnerValWithTs(v, ts) }
  }
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


  def putToKeyValue(put: PutRequest) = {
    new KeyValue(put.key(), put.family(), put.qualifier(), put.timestamp(), put.value())
  }
}
