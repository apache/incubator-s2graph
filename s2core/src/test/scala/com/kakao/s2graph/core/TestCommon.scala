package com.kakao.s2graph.core

import com.kakao.s2graph.core.mysqls._
//import com.kakao.s2graph.core.models._


import com.kakao.s2graph.core.types._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{PutRequest, KeyValue}


trait TestCommon {
  val ts = System.currentTimeMillis()
  val testServiceId = 1
  val testColumnId = 1
  val testLabelId = 1
  val testDir = GraphUtil.directions("out")
  val testOp = GraphUtil.operations("insert")
  val testLabelOrderSeq = LabelIndex.DefaultSeq
  val testLabelWithDir = LabelWithDirection(testLabelId, testDir)
  val labelMeta = LabelMeta

  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0

  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0

  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0

  def lessThanEqual(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) <= 0

  /** */
  import HBaseType.{VERSION2, VERSION1}
  private val tsValSmall = InnerVal.withLong(ts, VERSION1)
  private val tsValLarge = InnerVal.withLong(ts + 1, VERSION1)
  private val boolValSmall = InnerVal.withBoolean(false, VERSION1)
  private val boolValLarge = InnerVal.withBoolean(true, VERSION1)
  private val doubleValSmall = InnerVal.withDouble(-0.1, VERSION1)
  private val doubleValLarge = InnerVal.withDouble(0.1, VERSION1)
  private val toSeq = LabelMeta.toSeq.toInt
  private val toVal = InnerVal.withLong(Long.MinValue, VERSION1)


  private val tsValSmallV2 = InnerVal.withLong(ts, VERSION2)
  private val tsValLargeV2 = InnerVal.withLong(ts + 1, VERSION2)
  private val boolValSmallV2 = InnerVal.withBoolean(false, VERSION2)
  private val boolValLargeV2 = InnerVal.withBoolean(true, VERSION2)
  private val doubleValSmallV2 = InnerVal.withDouble(-0.1, VERSION2)
  private val doubleValLargeV2 = InnerVal.withDouble(0.1, VERSION2)
  private val toValV2 = InnerVal.withLong(Long.MinValue, VERSION2)

  val intVals = (Int.MinValue until Int.MinValue + 10) ++ (-129 to -126) ++ (-1 to 1) ++ (126 to 129) ++
    (Int.MaxValue - 10 until Int.MaxValue)
  val intInnerVals = intVals.map { v => InnerVal.withNumber(BigDecimal(v), VERSION1) }

  val intInnerValsV2 = intVals.map { v => InnerVal.withNumber(BigDecimal(v), VERSION2) }

  val stringVals = List("abc", "abd", "ac", "aca", "b")
  val stringInnerVals = stringVals.map { s => InnerVal.withStr(s, VERSION1)}
  val stringInnerValsV2 = stringVals.map { s => InnerVal.withStr(s, VERSION2)}

  val numVals = (Long.MinValue until Long.MinValue + 10).map(BigDecimal(_)) ++
    (Int.MinValue until Int.MinValue + 10).map(BigDecimal(_)) ++
    (Int.MaxValue - 10 until Int.MaxValue).map(BigDecimal(_)) ++
    (Long.MaxValue - 10 until Long.MaxValue).map(BigDecimal(_))
  val numInnerVals = numVals.map { n => InnerVal.withLong(n.toLong, VERSION1)}
  val numInnerValsV2 = numVals.map { n => InnerVal.withNumber(n, VERSION2)}

  val doubleStep = Double.MaxValue / 5
  val doubleVals = (Double.MinValue until 0 by doubleStep).map(BigDecimal(_)) ++
    (-9999.9 until -9994.1 by 1.1).map(BigDecimal(_)) ++
    (-128.0 until 128.0 by 1.2).map(BigDecimal(_)) ++
    (129.0 until 142.0 by 1.1).map(BigDecimal(_)) ++
    (doubleStep until Double.MaxValue by doubleStep).map(BigDecimal(_))
  val doubleInnerVals = doubleVals.map { d => InnerVal.withDouble(d.toDouble, VERSION1)}
  val doubleInnerValsV2 = doubleVals.map { d => InnerVal.withDouble(d.toDouble, VERSION2)}

  /** version 1 string order is broken */
  val idxPropsLs = Seq(
    Seq((0 -> tsValSmall), (1 -> boolValSmall), (2 -> InnerVal.withStr("ac", VERSION1)),(toSeq -> toVal)),
    Seq((0 -> tsValSmall), (1 -> boolValSmall), (2 -> InnerVal.withStr("ab", VERSION1)), (toSeq -> toVal)),
    Seq((0 -> tsValSmall), (1 -> boolValSmall), (2-> InnerVal.withStr("b", VERSION1)), (toSeq -> toVal)),
    Seq((0 -> tsValSmall), (1 -> boolValLarge), (2 -> InnerVal.withStr("b", VERSION1)), (toSeq -> toVal)),
    Seq((0 -> tsValLarge), (1 -> boolValSmall), (2 -> InnerVal.withStr("a", VERSION1)), (toSeq -> toVal))
  ).map(seq => seq.map(t => t._1.toByte -> t._2 ))

  val idxPropsLsV2 = Seq(
    Seq((0 -> tsValSmallV2), (1 -> boolValSmallV2), (2 -> InnerVal.withStr("a", VERSION2)), (3 -> doubleValSmallV2), (toSeq -> toValV2)),
    Seq((0 -> tsValSmallV2), (1 -> boolValSmallV2), (2 -> InnerVal.withStr("a", VERSION2)), (3 -> doubleValLargeV2), (toSeq -> toValV2)),
    Seq((0 -> tsValSmallV2), (1 -> boolValSmallV2), (2 -> InnerVal.withStr("ab", VERSION2)), (3 -> doubleValLargeV2), (toSeq -> toValV2)),
    Seq((0 -> tsValSmallV2), (1 -> boolValSmallV2), (2-> InnerVal.withStr("b", VERSION2)), (3 ->doubleValLargeV2), (toSeq -> toValV2)),
    Seq((0 -> tsValSmallV2), (1 -> boolValLargeV2), (2 -> InnerVal.withStr("a", VERSION2)), (3 ->doubleValLargeV2), (toSeq -> toValV2)),
    Seq((0 -> tsValLargeV2), (1 -> boolValSmallV2), (2 -> InnerVal.withStr("a", VERSION2)), (3 ->doubleValLargeV2), (toSeq -> toValV2))
  ).map(seq => seq.map(t => t._1.toByte -> t._2 ) )

  val idxPropsWithTsLs = idxPropsLs.map { idxProps =>
    idxProps.map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }
  }
  val idxPropsWithTsLsV2 = idxPropsLsV2.map { idxProps =>
    idxProps.map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }
  }
  //
  //  def testOrder(idxPropsLs: Seq[Seq[(Byte, InnerValLike)]],
  //                innerVals: Iterable[InnerValLike], skipHashBytes: Boolean = false)
  //               (createFunc: (Seq[(Byte, InnerValLike)], InnerValLike) => HBaseSerializable,
  //                fromBytesFunc: Array[Byte] => HBaseSerializable) = {
  //    /** check if increasing target vertex id is ordered properly with same indexProps */
  //    val rets = for {
  //      idxProps <- idxPropsLs
  //    } yield {
  //        val head = createFunc(idxProps, innerVals.head)
  //        val start = head
  //        var prev = head
  //        val rets = for {
  //          innerVal <- innerVals.tail
  //        } yield {
  //            val current = createFunc(idxProps, innerVal)
  //            val bytes = current.bytes
  //            val decoded = fromBytesFunc(bytes)
  //            println(s"current: $current")
  //            println(s"decoded: $decoded")
  //
  //            val prevBytes = if (skipHashBytes) prev.bytes.drop(GraphUtil.bytesForMurMurHash) else prev.bytes
  //            val currentBytes = if (skipHashBytes) bytes.drop(GraphUtil.bytesForMurMurHash) else bytes
  //            val (isSame, orderPreserved) = (current, decoded) match {
  //              case (c: v2.EdgeQualifier, d: v2.EdgeQualifier) if (idxProps.map(_._1).contains(toSeq)) =>
  //                /** _to is used in indexProps */
  //                (c.props.map(_._2) == d.props.map(_._2) && c.op == d.op, Bytes.compareTo(currentBytes, prevBytes) <= 0)
  //              case _ =>
  //                (current == decoded, lessThan(currentBytes, prevBytes))
  //            }
  //
  //            println(s"$current ${bytes.toList}")
  //            println(s"$prev ${prev.bytes.toList}")
  //            println(s"SerDe[$isSame], Order[$orderPreserved]")
  //            prev = current
  //            isSame && orderPreserved
  //          }
  //        rets.forall(x => x)
  //      }
  //    rets.forall(x => x)
  //  }
  //  def testOrderReverse(idxPropsLs: Seq[Seq[(Byte, InnerValLike)]], innerVals: Iterable[InnerValLike],
  //                       skipHashBytes: Boolean = false)
  //                      (createFunc: (Seq[(Byte, InnerValLike)], InnerValLike) => HBaseSerializable,
  //                       fromBytesFunc: Array[Byte] => HBaseSerializable) = {
  //    /** check if increasing target vertex id is ordered properly with same indexProps */
  //    val rets = for {
  //      innerVal <- innerVals
  //    } yield {
  //        val head = createFunc(idxPropsLs.head, innerVal)
  //        val start = head
  //        var prev = head
  //        val rets = for {
  //          idxProps <- idxPropsLs.tail
  //        } yield {
  //            val current = createFunc(idxProps, innerVal)
  //            val bytes = current.bytes
  //            val decoded = fromBytesFunc(bytes)
  //            println(s"current: $current")
  //            println(s"decoded: $decoded")
  //
  //            val prevBytes = if (skipHashBytes) prev.bytes.drop(GraphUtil.bytesForMurMurHash) else prev.bytes
  //            val currentBytes = if (skipHashBytes) bytes.drop(GraphUtil.bytesForMurMurHash) else bytes
  //            val (isSame, orderPreserved) = (current, decoded) match {
  //              case (c: v2.EdgeQualifier, d: v2.EdgeQualifier) if (idxProps.map(_._1).contains(toSeq)) =>
  //                /** _to is used in indexProps */
  //                (c.props.map(_._2) == d.props.map(_._2) && c.op == d.op, Bytes.compareTo(currentBytes, prevBytes) <= 0)
  //              case _ =>
  //                (current == decoded, lessThan(currentBytes, prevBytes))
  //            }
  //
  //            println(s"$current ${bytes.toList}")
  //            println(s"$prev ${prev.bytes.toList}")
  //            println(s"SerDe[$isSame], Order[$orderPreserved]")
  //            prev = current
  //            isSame && orderPreserved
  //          }
  //
  //        rets.forall(x => x)
  //      }
  //
  //    rets.forall(x => x)
  //  }
  //
  //
  //  def putToKeyValues(put: PutRequest) = {
  //    val ts = put.timestamp()
  //    for ((q, v) <- put.qualifiers().zip(put.values)) yield {
  //      new KeyValue(put.key(), put.family(), q, ts, v)
  //    }
  //  }
}
