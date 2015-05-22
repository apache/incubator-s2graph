package com.daumkakao.s2graph.core.models

import HBaseModel._

import scala.reflect.ClassTag

/**
 * Created by shon on 5/15/15.
 */

object HService {
  def findById(id: Int, useCache: Boolean = true): HService = {
    HBaseModel.find[HService](useCache)(Seq(("id" -> id))).get
  }
  def findByName(serviceName: String, useCache: Boolean = true): Option[HService] = {
    HBaseModel.find[HService](useCache)(Seq(("serviceName" -> serviceName)))
  }
  def findOrInsert(serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int],
                   useCache: Boolean = true): HService = {
    findByName(serviceName, useCache) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq[HService]
        val kvs = Map("id" -> id, "serviceName" -> serviceName, "cluster" -> cluster, "hbaseTableName" -> hTableName,
          "preSplitSize" -> preSplitSize, "hbaseTableTTL" -> hTableTTL.getOrElse(-1))
        val service = HService(kvs)
        service.create()
        service
    }
  }
  def findAllServices(): List[HService] = {
    HBaseModel.findsRange[HService](useCache = false)(Seq(("id"-> 0)), Seq(("id" -> Int.MaxValue)))
  }
}
case class HService(kvsParam: Map[KEY, VAL]) extends HBaseModel[HService]("HService", kvsParam) {
  override val columns = Seq("id", "serviceName", "cluster", "hbaseTableName", "preSplitSize", "hbaseTableTTL")

  val pk = Seq(("id", kvs("id")))
  val idxServiceName = Seq(("serviceName", kvs("serviceName")))
  val idxCluster = Seq(("cluster", kvs("cluster")))

  override val idxs = List(pk, idxServiceName, idxCluster)
  override def foreignKeys() = {
    List(
      HBaseModel.findsMatch[HServiceColumn](useCache = false)(Seq("serviceId" -> kvs("id"))),
      //HBaseModel.findsMatch[HLabel](useCache = false)(Seq("srcServiceId" -> kvs("id"))),
      //HBaseModel.findsMatch[HLabel](useCache = false)(Seq("tgtServiceId" -> kvs("id"))),
      HBaseModel.findsMatch[HLabel](useCache = false)(Seq("serviceId" -> kvs("id")))
    )
  }
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceName = kvs("serviceName").toString
  val cluster = kvs("cluster").toString
  val hTableName = kvs("hbaseTableName").toString
  val preSplitSize = kvs("preSplitSize").toString.toInt
  val hTableTTL = {
    val ttl = kvs("hbaseTableTTL").toString.toInt
    if (ttl < 0) None
    else Some(ttl)
  }
  lazy val toJson = kvs.toString
}

