package com.daumkakao.s2graph.core.models

import Model._
import com.daumkakao.s2graph.core.Management
import com.daumkakao.s2graph.core.types2.InnerVal
import play.api.Logger

import scala.reflect.ClassTag

/**
 * Created by shon on 5/15/15.
 */

object Service {
  def findById(id: Int, useCache: Boolean = true): Service = {
    try {
    Model.find[Service](useCache)(Seq(("id" -> id))).get
    } catch {
      case e: Throwable =>
        Logger.error(s"$id, $e", e)
        throw e
    }
  }
  def findByName(serviceName: String, useCache: Boolean = true): Option[Service] = {
    Model.find[Service](useCache)(Seq(("serviceName" -> serviceName)))
  }
  def findOrInsert(serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int],
                   useCache: Boolean = true): Service = {
    findByName(serviceName, useCache) match {
      case Some(s) => s
      case None =>
        val id = Model.getAndIncrSeq[Service]
        val kvs = Map("id" -> id, "serviceName" -> serviceName, "cluster" -> cluster, "hbaseTableName" -> hTableName,
          "preSplitSize" -> preSplitSize, "hbaseTableTTL" -> hTableTTL.getOrElse(-1))
        val service = Service(kvs)
        service.create()
        Management.createTable(cluster, hTableName, List("e", "v"), preSplitSize, hTableTTL)
        service
    }
  }
  def findAllServices(): List[Service] = {
    Model.findsRange[Service](useCache = false)(Seq(("id"-> 0)), Seq(("id" -> Int.MaxValue)))
  }
}
case class Service(kvsParam: Map[KEY, VAL]) extends Model[Service]("HService", kvsParam) {
  override val columns = Seq("id", "serviceName", "cluster", "hbaseTableName",
    "preSplitSize", "hbaseTableTTL")

  val pk = Seq(("id", kvs("id")))
  val idxServiceName = Seq(("serviceName", kvs("serviceName")))
  val idxCluster = Seq(("cluster", kvs("cluster")))

  override val idxs = List(pk, idxServiceName, idxCluster)
  override def foreignKeys() = {
    List(
      Model.findsMatch[ServiceColumn](useCache = false)(Seq("serviceId" -> kvs("id"))),
      //HBaseModel.findsMatch[HLabel](useCache = false)(Seq("srcServiceId" -> kvs("id"))),
      //HBaseModel.findsMatch[HLabel](useCache = false)(Seq("tgtServiceId" -> kvs("id"))),
      Model.findsMatch[Label](useCache = false)(Seq("serviceId" -> kvs("id")))
    )
  }
  validate(columns)
//  val schemaVersion = kvs.get("schemaVersion").getOrElse(InnerVal.DEFAULT_VERSION).toString

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

