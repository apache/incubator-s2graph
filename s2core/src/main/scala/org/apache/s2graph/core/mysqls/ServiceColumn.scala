/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */

import org.apache.s2graph.core.JSONParser
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.types.{HBaseType, InnerValLikeWithTs, InnerValLike}
import play.api.libs.json.Json
import scalikejdbc._
object ServiceColumn extends Model[ServiceColumn] {
  val Default = ServiceColumn(Option(0), -1, "default", "string", "v4")

  def apply(rs: WrappedResultSet): ServiceColumn = {
    ServiceColumn(rs.intOpt("id"), rs.int("service_id"), rs.string("column_name"), rs.string("column_type").toLowerCase(), rs.string("schema_version"))
  }

//  def findByServiceAndColumn(serviceName: String,
//                             columnName: String,
//                             useCache: Boolean  = true)(implicit session: DBSession): Option[ServiceColumn] = {
//    val service = Service.findByName(serviceName).getOrElse(throw new RuntimeException(s"$serviceName is not found."))
//    find(service.id.get, columnName, useCache)
//  }

  def findById(id: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession): ServiceColumn = {
    val cacheKey = "id=" + id

    if (useCache) {
      withCache(cacheKey)(sql"""select * from service_columns where id = ${id}""".map { x => ServiceColumn(x) }.single.apply).get
    } else {
      sql"""select * from service_columns where id = ${id}""".map { x => ServiceColumn(x) }.single.apply.get
    }
  }

  def find(serviceId: Int, columnName: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[ServiceColumn] = {
//    val cacheKey = s"serviceId=$serviceId:columnName=$columnName"
    val cacheKey = "serviceId=" + serviceId + ":columnName=" + columnName
    if (useCache) {
      withCache(cacheKey) {
        sql"""
          select * from service_columns where service_id = ${serviceId} and column_name = ${columnName}
        """.map { rs => ServiceColumn(rs) }.single.apply()
      }
    } else {
      sql"""
        select * from service_columns where service_id = ${serviceId} and column_name = ${columnName}
      """.map { rs => ServiceColumn(rs) }.single.apply()
    }
  }
  def insert(serviceId: Int, columnName: String, columnType: Option[String], schemaVersion: String)(implicit session: DBSession = AutoSession) = {
    sql"""insert into service_columns(service_id, column_name, column_type, schema_version)
         values(${serviceId}, ${columnName}, ${columnType}, ${schemaVersion})""".execute.apply()
  }
  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val serviceColumn = findById(id, useCache = false)
    val (serviceId, columnName) = (serviceColumn.serviceId, serviceColumn.columnName)
    sql"""delete from service_columns where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"serviceId=$serviceId:columnName=$columnName")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String], schemaVersion: String = HBaseType.DEFAULT_VERSION, useCache: Boolean = true)(implicit session: DBSession = AutoSession): ServiceColumn = {
    find(serviceId, columnName, useCache) match {
      case Some(sc) => sc
      case None =>
        insert(serviceId, columnName, columnType, schemaVersion)
//        val cacheKey = s"serviceId=$serviceId:columnName=$columnName"
        val cacheKey = "serviceId=" + serviceId + ":columnName=" + columnName
        expireCache(cacheKey)
        find(serviceId, columnName).get
    }
  }
  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from service_columns""".map { rs => ServiceColumn(rs) }.list.apply
    putsToCache(ls.map { x =>
      var cacheKey = s"id=${x.id.get}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      var cacheKey = s"serviceId=${x.serviceId}:columnName=${x.columnName}"
      (cacheKey -> x)
    })
    ls
  }
}
case class ServiceColumn(id: Option[Int],
                         serviceId: Int,
                         columnName: String,
                         columnType: String,
                         schemaVersion: String)  {

  lazy val service = Service.findById(serviceId)
  lazy val metas = ColumnMeta.timestamp +: ColumnMeta.findAllByColumn(id.get) :+ ColumnMeta.lastModifiedAtColumn
  lazy val metasMap = metas.map { meta => meta.seq.toInt -> meta } toMap
  lazy val metasInvMap = metas.map { meta => meta.name -> meta} toMap
  lazy val metaNamesMap = (ColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq.toInt, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)

  def propsToInnerVals(props: Map[String, Any]): Map[ColumnMeta, InnerValLike] = {
    for {
      (k, v) <- props
      labelMeta <- metasInvMap.get(k)
      innerVal = toInnerVal(v, labelMeta.dataType, schemaVersion)
    } yield labelMeta -> innerVal
  }

  def innerValsToProps(props: Map[Int, InnerValLike]): Map[String, Any] = {
    for {
      (k, v) <- props
      columnMeta <- metasMap.get(k)
    } yield {
      columnMeta.name -> v.value
    }
  }

  def innerValsWithTsToProps(props: Map[Int, InnerValLikeWithTs]): Map[String, Any] = {
    for {
      (k, v) <- props
      columnMeta <- metasMap.get(k)
    } yield {
      columnMeta.name -> v.innerVal.value
    }
  }


}
