//package com.daumkakao.s2graph.core
//
//import com.daumkakao.s2graph.core.models._
//import com.daumkakao.s2graph.core.types2._
//import org.apache.hadoop.hbase.util.Bytes
//import org.hbase.async.{PutRequest, KeyValue}
//
//
///**
// * Created by shon on 6/1/15.
// */
//trait TestCommon {
//
//
//
//  val ts = System.currentTimeMillis()
//  val testServiceId = 1
//  val testColumnId = 1
//  val testLabelId = 1
//  val testDir = GraphUtil.directions("out")
//  val testOp = GraphUtil.operations("insert")
//  val testLabelOrderSeq = LabelIndex.defaultSeq
//  val testLabelWithDir = LabelWithDirection(testLabelId, testDir)
//
//
//
//  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0
//
//  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0
//
//  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0
//
//  def lessThanEqual(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) <= 0
//
//  /** */
//  val version = "v2"
//  private val tsVal1 = InnerVal.withLong(ts, version)
//  private val tsVal2 = InnerVal.withLong(ts + 1, version)
//  private val boolVal1 = InnerVal.withBoolean(false, version)
//  private val boolVal2 = InnerVal.withBoolean(true, version)
//  private val doubleVal1 = InnerVal.withDouble(-0.1, version)
//  private val doubleVal2 = InnerVal.withDouble(0.1, version)
//  private val toSeq = LabelMeta.toSeq.toInt
//  private val toVal = InnerVal.withLong(Long.MinValue, version)
//
//  val intVals = {
//    val vals = (Int.MinValue until Int.MinValue + 10) ++
//      (-128 to 128) ++ (Int.MaxValue - 10 until Int.MaxValue)
//    vals.map { v => InnerVal.withNumber(BigDecimal(v), version) }
//  }
//  val idxPropsLs = Seq(
//    Seq((0 -> tsVal1), (1 -> boolVal1), (2 -> InnerVal.withStr("a", version), (3 -> doubleVal1), (toSeq -> toVal)),
//    Seq((0 -> tsVal1), (1 -> boolVal1), (2 -> InnerVal.withStr("a", version), (3 -> doubleVal2), (toSeq -> toVal)),
//    Seq((0 -> tsVal1), (1 -> boolVal1), (2 -> InnerVal.withStr("ab", version), (3 -> doubleVal2), (toSeq -> toVal)),
//    Seq((0 -> tsVal1), (1 -> boolVal1), (2-> InnerVal.withStr("b", version), (3 ->doubleVal2), (toSeq -> toVal)),
//    Seq((0 -> tsVal1), (1 -> boolVal2), (2 -> InnerVal.withStr("a", version), (3 ->doubleVal2), (toSeq -> toVal)),
//    Seq((0 -> tsVal2), (1 -> boolVal1), (2 -> InnerVal.withStr("a", version), (3 ->doubleVal2), (toSeq -> toVal))
//  ).map(seq => seq.map(t => t._1.toByte -> t._2 ) )
//
//  val idxPropsWithTsLs = idxPropsLs.map { idxProps =>
//    idxProps.map { case (k, v) => k -> InnerValLikeWithTs(v, ts) }
//  }
//  def testOrder(idxPropsLs: Seq[Seq[(Byte, InnerValLike)]], innerVals: Iterable[InnerValLike], useHash: Boolean = false)
//               (createFunc: (Seq[(Byte, InnerValLike)], InnerValLike) => HBaseDeserializable,
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
//            val prevBytes = if (useHash) prev.bytes.drop(GraphUtil.bytesForMurMurHash) else prev.bytes
//            val currentBytes = if (useHash) bytes.drop(GraphUtil.bytesForMurMurHash) else bytes
//            val (isSame, orderPreserved) = (current, decoded) match {
//              case (c: EdgeQualifier, d: EdgeQualifier) if (c.tgtVertexId == null | d.tgtVertexId == null) =>
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
//  def testOrderReverse(idxPropsLs: Seq[Seq[(Byte, InnerVal)]], innerVals: Iterable[InnerVal], useHash: Boolean = false)
//                      (createFunc: (Seq[(Byte, InnerVal)], InnerVal) => HBaseType,
//                       fromBytesFunc: Array[Byte] => HBaseType) = {
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
//            val prevBytes = if (useHash) prev.bytes.drop(GraphUtil.bytesForMurMurHash) else prev.bytes
//            val currentBytes = if (useHash) bytes.drop(GraphUtil.bytesForMurMurHash) else bytes
//            val (isSame, orderPreserved) = (current, decoded) match {
//              case (c: EdgeQualifier, d: EdgeQualifier) if (c.tgtVertexId == null | d.tgtVertexId == null) =>
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
//  def putToKeyValue(put: PutRequest) = {
//    new KeyValue(put.key(), put.family(), put.qualifier(), put.timestamp(), put.value())
//  }
//}
