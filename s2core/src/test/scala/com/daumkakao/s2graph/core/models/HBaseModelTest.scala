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
  HBaseModel(zkQuorum)

//  test("test HColumnMeta") {
//    val kvs = Map("id" -> 1, "columnId" -> 10, "name" -> "testMeta", "seq" -> 4.toByte)
//    val model = HColumnMeta(kvs)
//    println(model.create())
//    println(HColumnMeta.findById(1, useCache = false))
//
//  }
  test("test HService") {
    for ((serviceName, id) <- List("s2a", "s2graph", "s2zz", "s3a", "s3z").zipWithIndex) {
      val kvs = Map("id" -> id, "serviceName" -> serviceName, "cluster" -> "localhost",
      "hbaseTableName" -> "s2graph-dev", "preSplitSize" -> 0, "hbaseTableTTL" -> -1)
      val model = HService(kvs)
      println(model.create())
      println(HService.findById(id))
      model.update("serviceName", s"new_$serviceName")
      println(HService.findById(id))
    }
    val finds = HBaseModel.findsMatch[HService](useCache = false)_
    println(finds(Seq(("serviceName", "s2graph"))))
  }
//  test("test HServiceColumn") {
//    val kvs = Map("id" -> "1", "serviceId" -> "10", "columnName" -> "testColumnName", "columnType" -> "long")
//    val model = HServiceColumn(kvs)
//    println(model.create(zkQuorum))
//    val find = HBaseModel.find(zkQuorum)("HServiceColumn")_
//    find(Seq(("id", "1"))) ==
//      find(Seq(("serviceId", "10"), ("columnName", "testColumnName")))
//  }
//  test("test HLabelIndex") {
//    val find = HBaseModel.find(zkQuorum)("HLabelIndex")_
//    val finds = HBaseModel.finds(zkQuorum)("HLabelIndex")_
//
//    val models = for (seq <- (1 until 4)) yield {
//      val kvs = Map("id" -> s"${10 + seq}", "labelId" -> "1", "seq" -> s"$seq",
//        "metaSeqs" -> (0 until seq).toList.map (x => "a").mkString(","),
//        "formular" -> "")
//
//      val model = HLabelIndex(kvs)
//      println(model.create(zkQuorum))
//      println(find(Seq(("id", s"${10 + seq}"))))
//    }
//    println(finds(Seq(("labelId", "1")), Seq(("labelId", "2"))))
//  }
//  test("test HLabelMeta") {
//    val model = HLabelMeta(1, 23, "testName", 1.toByte, "null", "string", true)
//    println(model.insert(zkQuorum))
//    println(model.find(zkQuorum)(model.pk))
//    println(model.find(zkQuorum)(model.idxByLabelIdName))
//    println(model.find(zkQuorum)(model.idxByLabelIdSeq))
//  }

}
