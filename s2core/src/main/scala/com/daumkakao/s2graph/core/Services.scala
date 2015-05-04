package com.daumkakao.s2graph.core

import play.api.libs.json.Json
import scalikejdbc._

object Service extends Model[Service] {

  private val adminLogger = Logger.adminLogger

  def apply(rs: WrappedResultSet): Service = {
    Service(rs.intOpt("id"), rs.string("service_name"), rs.string("cluster"), rs.string("hbase_table_name"), rs.int("pre_split_size"), rs.intOpt("hbase_table_ttl"))
  }

  def findById(id: Int): Service = {
    val cacheKey = s"id=$id"
    withCache(cacheKey)(sql"""select * from services where id = ${id}""".map { rs => Service(rs) }.single.apply).get
  }
  def findByName(serviceName: String): Option[Service] = {
    val cacheKey = s"serviceName=$serviceName"
    withCache(cacheKey)(sql"""
        select * from services where service_name = ${serviceName}
    """.map { rs => Service(rs) }.single.apply())
  }
  def insert(serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]) = {
    adminLogger.info(s"$serviceName, $cluster, $hTableName, $preSplitSize, $hTableTTL")
    sql"""insert into services(service_name, cluster, hbase_table_name, pre_split_size, hbase_table_ttl) 
    values(${serviceName}, ${cluster}, ${hTableName}, ${preSplitSize}, ${hTableTTL})""".execute.apply()
    Management.createTable(cluster, hTableName, List("e", "v"), preSplitSize, hTableTTL)
  }
  def delete(id: Int) = {
    val service = findById(id)
    val serviceName = service.serviceName
    sql"""delete from service_columns where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"serviceName=$serviceName")
    cacheKeys.foreach(expireCache(_))
  }
  def findOrInsert(serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]): Service = {
    findByName(serviceName) match {
      case Some(s) => s
      case None =>
        insert(serviceName, cluster, hTableName, preSplitSize, hTableTTL)
        val cacheKey = s"serviceName=$serviceName"
        expireCache(cacheKey)
        findByName(serviceName).get
    }
  }
  def findAllServices(): List[Service] = {
    val services = sql"""select * from services""".map { rs => Service(rs) }.list.apply
    putsToCache(services.map { service =>
      val cacheKey = s"serviceName=${service.serviceName}"
      (cacheKey -> service)
    })
    services
  }
  def findAllConn(): List[String] = {
    sql"""select distinct(cluster) from services""".map { rs => rs.string("cluster") }.list.apply
  }
}
case class Service(id: Option[Int], serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]) {
  lazy val toJson =
    id match {
      case Some(_id) =>
        Json.obj("id" -> _id, "name" -> serviceName, "hbaseTableName" -> hTableName, "preSplitSize" -> preSplitSize, "hTableTTL" -> hTableTTL)
      case None =>
        Json.parse("{}")
    }
}