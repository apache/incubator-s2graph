package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.models.HBaseModel
import com.daumkakao.s2graph.core.types.{CompositeId, InnerVal}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{KeyValue, PutRequest}
import org.scalatest.{Matchers, FunSuite}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 5/29/15.
 */
class VertexTest extends FunSuite with Matchers  {

  val zkQuorum = "localhost"
  val config = ConfigFactory.parseString(s"hbase.zookeeper.quorum=$zkQuorum")
  Graph(config)(ExecutionContext.Implicits.global)
  HBaseModel(zkQuorum)

  val columnId = 1
  val op = 0.toByte
  val ts = System.currentTimeMillis
  val intVals = {
    val vals = (Int.MinValue until Int.MinValue + 10) ++
      (-128 to 128) ++ (Int.MaxValue - 10 until Int.MaxValue)
    vals.map { v => InnerVal(BigDecimal(v)) }
  }
  val propsLs = Seq(
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("a"))),
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("ab"))),
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("b"))),
    Seq((0.toByte -> InnerVal.withLong(ts)), (1.toByte -> InnerVal.withLong(11)), (2.toByte -> InnerVal.withStr("a"))),
    Seq((0.toByte -> InnerVal.withLong(ts + 1)), (1.toByte -> InnerVal.withLong(10)), (2.toByte -> InnerVal.withStr("a")))
  )
  def putToKeyValue(put: PutRequest) = {
    new KeyValue(put.key(), put.family(), put.qualifier(), put.timestamp(), put.value())
  }
  def equalsExact(left: Vertex, right: Vertex) = {
    println(left, right)
    left.id == right.id && left.ts == right.ts &&
    left.props == right.props && left.op == right.op
  }
  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0
  /** assumes innerVal is sorted */
  def testOrder(innerVals: Seq[InnerVal],
                propsLs: Seq[Seq[(Byte, InnerVal)]]) = {
    val rets = for {
      props <- propsLs
    } yield {
      val head = Vertex(CompositeId(columnId, innerVals.head, isEdge = false, useHash = true),
      ts, props.toMap, op)
      val start = head
      var prev = head
      val rets = for {
        innerVal <- innerVals.tail
      } yield {
          var current = Vertex(CompositeId(columnId, innerVal, false, true), ts, props.toMap, op)
          val puts = current.buildPutsAsync()
          val kvs = for { put <- puts } yield {
            putToKeyValue(put)
          }
          val decodedOpt = Vertex(kvs)
          val comp = decodedOpt.isDefined &&
          equalsExact(decodedOpt.get, current) &&
          largerThan(current.rowKey.bytes, prev.rowKey.bytes) &&
          largerThan(current.rowKey.bytes, start.rowKey.bytes)

          prev = current
          comp
        }
      rets.forall(x => x)
    }
    rets.forall(x => x)
  }
  test("test with different innerVals as id") {
    testOrder(intVals, propsLs)
  }

}
