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

import play.api.libs.json.Json
import scalikejdbc._

import scala.util.Try

object ColumnMeta extends SQLSyntaxSupport[ColumnMeta] {
  import Schema._
  val className = ColumnMeta.getClass.getSimpleName

  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val lastModifiedAtColumnSeq = 0.toByte
  val lastModifiedAtColumn = ColumnMeta(Some(0), 0, "lastModifiedAt", lastModifiedAtColumnSeq, "long", "-1")
  val maxValue = Byte.MaxValue

  val timestamp = ColumnMeta(None, -1, "_timestamp", timeStampSeq.toByte, "long", "-1")
  val reservedMetas = Seq(timestamp, lastModifiedAtColumn)
  val reservedMetaNamesSet = reservedMetas.map(_.name).toSet

  def isValid(columnMeta: ColumnMeta): Boolean =
    columnMeta.id.isDefined && columnMeta.id.get > 0 && columnMeta.seq >= 0

  def valueOf(rs: WrappedResultSet): ColumnMeta = {
    ColumnMeta(Some(rs.int("id")), rs.int("column_id"), rs.string("name"),
      rs.byte("seq"), rs.string("data_type").toLowerCase(), rs.string("default_value"), rs.boolean("store_in_global_index"))
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession) = {
    val cacheKey = className + "id=" + id
    withCache(cacheKey) {
      sql"""select * from column_metas where id = ${id}""".map { rs => ColumnMeta.valueOf(rs) }.single.apply
    }.get
  }

  def findAllByColumn(columnId: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    //    val cacheKey = s"columnId=$columnId"
    val cacheKey = className + "columnId=" + columnId
    if (useCache) {
      withCaches(cacheKey)( sql"""select *from column_metas where column_id = ${columnId} order by seq ASC"""
        .map { rs => ColumnMeta.valueOf(rs) }.list.apply())
    } else {
      sql"""select * from column_metas where column_id = ${columnId} order by seq ASC"""
        .map { rs => ColumnMeta.valueOf(rs) }.list.apply()
    }
  }

  def findByName(columnId: Int, name: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    //    val cacheKey = s"columnId=$columnId:name=$name"
    val cacheKey = className + "columnId=" + columnId + ":name=" + name
    lazy val sql =  sql"""select * from column_metas where column_id = ${columnId} and name = ${name}"""
      .map { rs => ColumnMeta.valueOf(rs) }.single.apply()

    if (useCache) withCache(cacheKey)(sql)
    else sql
  }

  def insert(columnId: Int, name: String, dataType: String, defaultValue: String, storeInGlobalIndex: Boolean = false)(implicit session: DBSession = AutoSession) = {
    val ls = findAllByColumn(columnId, false)
    val seq = ls.size + 1
    if (seq <= maxValue) {
      sql"""insert into column_metas(column_id, name, seq, data_type, default_value, store_in_global_index)
    select ${columnId}, ${name}, ${seq}, ${dataType}, ${defaultValue}, ${storeInGlobalIndex}"""
        .updateAndReturnGeneratedKey.apply()
    }
  }

  def findOrInsert(columnId: Int,
                   name: String,
                   dataType: String,
                   defaultValue: String,
                   storeInGlobalIndex: Boolean = false,
                   useCache: Boolean = true)(implicit session: DBSession = AutoSession): ColumnMeta = {
    findByName(columnId, name, useCache) match {
      case Some(c) => c
      case None =>
        insert(columnId, name, dataType, defaultValue, storeInGlobalIndex)
        expireCache(className + s"columnId=$columnId:name=$name")
        findByName(columnId, name).get
    }
  }

  def findByIdAndSeq(columnId: Int, seq: Byte, useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    val cacheKey = className + "columnId=" + columnId + ":seq=" + seq
    lazy val columnMetaOpt = sql"""
        select * from column_metas where column_id = ${columnId} and seq = ${seq}
    """.map { rs => ColumnMeta.valueOf(rs) }.single.apply()

    if (useCache) withCache(cacheKey)(columnMetaOpt)
    else columnMetaOpt
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val columnMeta = findById(id)
    val (columnId, name) = (columnMeta.columnId, columnMeta.name)
    sql"""delete from column_metas where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"columnId=$columnId:name=$name", s"colunmId=$columnId")
    cacheKeys.foreach { key =>
      expireCache(className + key)
      expireCaches(className + key)
    }
  }

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from column_metas""".map { rs => ColumnMeta.valueOf(rs) }.list().apply()

    putsToCacheOption(ls.flatMap { x =>
      Seq(
        s"id=${x.id.get}",
        s"columnId=${x.columnId}:name=${x.name}",
        s"columnId=${x.columnId}:seq=${x.seq}"
      ).map(cacheKey => (className + cacheKey, x))
    })

    putsToCaches(ls.groupBy(x => x.columnId).map { case (columnId, ls) =>
      val cacheKey = className + s"columnId=${columnId}"
      (cacheKey -> ls)
    }.toList)

    ls
  }

  def updateStoreInGlobalIndex(id: Int, storeInGlobalIndex: Boolean)(implicit session: DBSession = AutoSession): Try[Long] = Try {
    sql"""
          update column_metas set store_in_global_index = ${storeInGlobalIndex} where id = ${id}
       """.updateAndReturnGeneratedKey.apply()
  }
}

case class ColumnMeta(id: Option[Int],
                      columnId: Int,
                      name: String,
                      seq: Byte,
                      dataType: String,
                      defaultValue: String,
                      storeInGlobalIndex: Boolean = false) {
  lazy val toJson = Json.obj("name" -> name, "dataType" -> dataType)
  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[ColumnMeta]) false
    else {
      val o = other.asInstanceOf[ColumnMeta]
      //      labelId == o.labelId &&
      seq == o.seq
    }
  }
  override def hashCode(): Int = seq.toInt
}
