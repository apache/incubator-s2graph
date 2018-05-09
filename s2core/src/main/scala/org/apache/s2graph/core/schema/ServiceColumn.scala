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

package org.apache.s2graph.core.schema

import com.typesafe.config.Config
import org.apache.s2graph.core.JSONParser
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.types.{HBaseType, InnerValLike, InnerValLikeWithTs}
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.Json
import scalikejdbc._

object ServiceColumn extends SQLSyntaxSupport[ServiceColumn] {
  import Schema._
  val className = ServiceColumn.getClass.getSimpleName

  val Default = ServiceColumn(Option(0), -1, "default", "string", "v4", None)

  def valueOf(rs: WrappedResultSet): ServiceColumn = {
    ServiceColumn(rs.intOpt("id"), rs.int("service_id"), rs.string("column_name"),
      rs.string("column_type").toLowerCase(), rs.string("schema_version"), rs.stringOpt("options"))
  }

  def findByServiceId(serviceId: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Seq[ServiceColumn] = {
    val cacheKey = "serviceId=" + serviceId

    lazy val sql = sql"""select * from service_columns where service_id = ${serviceId}""".map { x => ServiceColumn.valueOf(x) }.list().apply()

    if (useCache) withCaches(cacheKey)(sql)
    else sql
  }

  def findById(id: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession): ServiceColumn = {
    val cacheKey = className + "id=" + id
    lazy val sql = sql"""select * from service_columns where id = ${id}""".map { x => ServiceColumn.valueOf(x) }.single.apply
    if (useCache) withCache(cacheKey)(sql).get
    else sql.get
  }

  def find(serviceId: Int, columnName: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[ServiceColumn] = {
    val cacheKey = className + "serviceId=" + serviceId + ":columnName=" + columnName
    if (useCache) {
      withCache(cacheKey) {
        sql"""
          select * from service_columns where service_id = ${serviceId} and column_name = ${columnName}
        """.map { rs => ServiceColumn.valueOf(rs) }.single.apply()
      }
    } else {
      sql"""
        select * from service_columns where service_id = ${serviceId} and column_name = ${columnName}
      """.map { rs => ServiceColumn.valueOf(rs) }.single.apply()
    }
  }
  def insert(serviceId: Int, columnName: String, columnType: Option[String], schemaVersion: String, options: Option[String])(implicit session: DBSession = AutoSession) = {
    sql"""insert into service_columns(service_id, column_name, column_type, schema_version, options)
         values(${serviceId}, ${columnName}, ${columnType}, ${schemaVersion}, ${options})""".execute.apply()
  }
  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val serviceColumn = findById(id, useCache = false)
    val (serviceId, columnName) = (serviceColumn.serviceId, serviceColumn.columnName)
    sql"""delete from service_columns where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"serviceId=$serviceId:columnName=$columnName")
    cacheKeys.foreach { key =>
      expireCache(className + key)
      expireCaches(className + key)
    }
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String],
                   schemaVersion: String = HBaseType.DEFAULT_VERSION,
                   options: Option[String],
                   useCache: Boolean = true)(implicit session: DBSession = AutoSession): ServiceColumn = {
    find(serviceId, columnName, useCache) match {
      case Some(sc) => sc
      case None =>
        insert(serviceId, columnName, columnType, schemaVersion, options)
//        val cacheKey = s"serviceId=$serviceId:columnName=$columnName"
        val cacheKey = "serviceId=" + serviceId + ":columnName=" + columnName
        expireCache(className + cacheKey)
        find(serviceId, columnName).get
    }
  }
  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from service_columns""".map { rs => ServiceColumn.valueOf(rs) }.list.apply
    putsToCacheOption(ls.flatMap { x =>
      Seq(
        s"id=${x.id.get}",
        s"serviceId=${x.serviceId}:columnName=${x.columnName}"
      ).map(cacheKey => (className + cacheKey, x))
    })

    ls
  }
  def updateOption(serviceColumn: ServiceColumn, options: String)(implicit session: DBSession = AutoSession) = {
    scala.util.Try(Json.parse(options)).getOrElse(throw new RuntimeException("invalid Json option"))
    logger.info(s"update options of service column ${serviceColumn.service.serviceName} ${serviceColumn.columnName}, ${options}")
    val cnt = sql"""update service_columns set options = $options where id = ${serviceColumn.id.get}""".update().apply()
    val column = findById(serviceColumn.id.get, useCache = false)

    val cacheKeys = List(s"id=${column.id.get}",
      s"serviceId=${serviceColumn.serviceId}:columnName=${serviceColumn.columnName}")

    cacheKeys.foreach { key =>
      expireCache(className + key)
      expireCaches(className + key)
    }

    column
  }
}
case class ServiceColumn(id: Option[Int],
                         serviceId: Int,
                         columnName: String,
                         columnType: String,
                         schemaVersion: String,
                         options: Option[String])  {

  lazy val service = Service.findById(serviceId)
  lazy val metasWithoutCache = ColumnMeta.timestamp +: ColumnMeta.findAllByColumn(id.get, false) :+ ColumnMeta.lastModifiedAtColumn
  lazy val metas = ColumnMeta.timestamp +: ColumnMeta.findAllByColumn(id.get) :+ ColumnMeta.lastModifiedAtColumn
  lazy val metasMap = metas.map { meta => meta.seq.toInt -> meta } toMap
  lazy val metasInvMap = metas.map { meta => meta.name -> meta} toMap
  lazy val metaNamesMap = (ColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq.toInt, x.name)) toMap
  lazy val metaPropsDefaultMap = metas.map { meta =>
    meta -> JSONParser.toInnerVal(meta.defaultValue, meta.dataType, schemaVersion)
  }.toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)

  def propsToInnerVals(props: Map[String, Any]): Map[ColumnMeta, InnerValLike] = {
    val ret = for {
      (k, v) <- props
      labelMeta <- metasInvMap.get(k)
      innerVal = toInnerVal(v, labelMeta.dataType, schemaVersion)
    } yield labelMeta -> innerVal

    ret
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

  lazy val extraOptions = Schema.extraOptions(options)

  def toFetcherConfig: Option[Config] = {
    Schema.toConfig(extraOptions, "fetcher")
  }

  def toMutatorConfig: Option[Config] = {
    Schema.toConfig(extraOptions, "mutator")
  }
}
