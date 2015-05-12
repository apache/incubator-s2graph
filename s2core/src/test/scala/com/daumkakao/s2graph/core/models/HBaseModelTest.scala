package com.daumkakao.s2graph.core.models

import java.util.concurrent.ExecutorService

import com.daumkakao.s2graph.core.Graph
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 5/12/15.
 */
class HBaseModelTest extends FunSuite with Matchers {

  val zkQuorum = "localhost"
  val config = ConfigFactory.parseString(s"hbase.zookeeper.quorum=$zkQuorum")
  Graph(config)(ExecutionContext.Implicits.global)

  test("test HColumnMeta") {
    val model = HColumnMeta(1, 10, "testModel", 0.toByte)
    println(model.insert(zkQuorum = zkQuorum))
    println(model.find(zkQuorum)(model.pk))
    println(model.find(zkQuorum)(model.idxByColumnIdSeq))
  }
  test("test HService") {
    val model = HService(1, "testServiceName", "testCluster", "testTable", 10, 86000)
    println(model.insert(zkQuorum))
    println(model.find(zkQuorum)(model.pk))
    println(model.find(zkQuorum)(model.idxByServiceName))
    println(model.find(zkQuorum)(model.idxByCluster))
  }
  test("test HServiceColumn") {
    val model = HServiceColumn(1, 10, "testColumn", "string")
    println(model.insert(zkQuorum))
    println(model.find(zkQuorum)(model.pk))
    println(model.find(zkQuorum)(model.idxByServiceIdColumnName))
  }
  test("test HLabelMeta") {
    val model = HLabelMeta(1, 23, "testName", 1.toByte, "null", "string", true)
    println(model.insert(zkQuorum))
    println(model.find(zkQuorum)(model.pk))
    println(model.find(zkQuorum)(model.idxByLabelIdName))
    println(model.find(zkQuorum)(model.idxByLabelIdSeq))
  }
  test("test HLabelIndex") {
    val models = for (seq <- (0 until 4)) yield {
      HLabelIndex(10 + seq, 32, seq.toByte, s"a,b,c")
    }
    models.foreach { model => model.insert(zkQuorum) }
    val head = models.head
    println(head.find(zkQuorum)(head.pk))
    println(head.finds(zkQuorum)(head.pk))
  }
}
