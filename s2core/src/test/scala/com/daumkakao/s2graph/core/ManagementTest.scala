package com.daumkakao.s2graph.core

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, FunSuite}

import scala.concurrent.ExecutionContext

/**
 * Created by shon on 5/18/15.
 */
class ManagementTest extends FunSuite with Matchers {
  val zkQuorum = "tokyo062.kr2.iwilab.com"
  val database = "jdbc:mysql://nuk151.kr2.iwilab.com:13306/graph_alpha"
  val config = ConfigFactory.parseString(
    s"""
       | db.default.url="$database"
       | cache.ttl.second=60
       | cache.max.size=1000
       | hbase.client.operation.timeout=1000
       | hbase.zookeeper.quorum="$zkQuorum"
     """.stripMargin)
  val labelToCopy = "graph_test"
  val newLabelName = "graph_test_tc"
  val hTableName = "graph_test_tc"
  Graph.apply(config)(ExecutionContext.Implicits.global)
  test("test copy label") {
    Management.copyLabel(labelToCopy, newLabelName, Some(hTableName))
  }
}
