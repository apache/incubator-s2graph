package com.daumkakao.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */

import com.daumkakao.s2graph.core.JSONParser
import scalikejdbc._
import play.api.libs.json.Json
object ServiceColumn extends Model[ServiceColumn] {

  def apply(rs: WrappedResultSet): ServiceColumn = {
    ServiceColumn(rs.intOpt("id"), rs.int("service_id"), rs.string("column_name"), rs.string("column_type"), rs.string("schema_version"))
  }

  def findById(id: Int): ServiceColumn = {
    val cacheKey = s"id=$id"
    withCache(cacheKey)(sql"""select * from service_columns where id = ${id}""".map { x => ServiceColumn(x) }.single.apply).get
  }
  def find(serviceId: Int, columnName: String, useCache: Boolean = true): Option[ServiceColumn] = {
    val cacheKey = s"serviceId=$serviceId:columnName=$columnName"
    val fetch = sql"""
        select * from service_columns where service_id = ${serviceId} and column_name = ${columnName}
    """.map { rs => ServiceColumn(rs) }.single.apply()
    if (useCache) {
      withCache(cacheKey)(fetch)
    } else {
      fetch
    }
  }
  def insert(serviceId: Int, columnName: String, columnType: Option[String], schemaVersion: String) = {
    sql"""insert into service_columns(service_id, column_name, column_type, schema_version)
         values(${serviceId}, ${columnName}, ${columnType}, ${schemaVersion})""".execute.apply()
  }
  def delete(id: Int) = {
    val serviceColumn = findById(id)
    val (serviceId, columnName) = (serviceColumn.serviceId, serviceColumn.columnName)
    sql"""delete from service_columns where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"serviceId=$serviceId:columnName=$columnName")
    cacheKeys.foreach(expireCache(_))
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String], schemaVersion: String): ServiceColumn = {
    find(serviceId, columnName) match {
      case Some(sc) => sc
      case None =>
        insert(serviceId, columnName, columnType, schemaVersion)
        val cacheKey = s"serviceId=$serviceId:columnName=$columnName"
        expireCache(cacheKey)
        find(serviceId, columnName).get
    }
  }
}
case class ServiceColumn(id: Option[Int], serviceId: Int, columnName: String, columnType: String, schemaVersion: String) extends JSONParser {

  lazy val service = Service.findById(serviceId)
  lazy val metas = ColumnMeta.findAllByColumn(id.get)
  lazy val metasInvMap = metas.map { meta => meta.name -> meta} toMap
  lazy val metaNamesMap = (ColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)


}
