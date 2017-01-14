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

import java.util.UUID

import com.typesafe.config.Config
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.Json
import scalikejdbc._

object Service extends Model[Service] {
  def apply(rs: WrappedResultSet): Service = {
    Service(rs.intOpt("id"), rs.string("service_name"), rs.string("access_token"),
      rs.string("cluster"), rs.string("hbase_table_name"), rs.int("pre_split_size"), rs.intOpt("hbase_table_ttl"))
  }

  def findByAccessToken(accessToken: String)(implicit session: DBSession = AutoSession): Option[Service] = {
    val cacheKey = s"accessToken=$accessToken"
    withCache(cacheKey)( sql"""select * from services where access_token = ${accessToken}""".map { rs => Service(rs) }.single.apply)
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession): Service = {
    val cacheKey = "id=" + id
    withCache(cacheKey)( sql"""select * from services where id = ${id}""".map { rs => Service(rs) }.single.apply).get
  }

  def findByName(serviceName: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[Service] = {
    val cacheKey = "serviceName=" + serviceName
    lazy val serviceOpt = sql"""
        select * from services where service_name = ${serviceName}
      """.map { rs => Service(rs) }.single.apply()

    if (useCache) withCache(cacheKey)(serviceOpt)
    else serviceOpt
  }

  def insert(serviceName: String, cluster: String,
             hTableName: String, preSplitSize: Int, hTableTTL: Option[Int],
             compressionAlgorithm: String)(implicit session: DBSession = AutoSession): Unit = {
    logger.info(s"$serviceName, $cluster, $hTableName, $preSplitSize, $hTableTTL, $compressionAlgorithm")
    val accessToken = UUID.randomUUID().toString()
    sql"""insert into services(service_name, access_token, cluster, hbase_table_name, pre_split_size, hbase_table_ttl)
    values(${serviceName}, ${accessToken}, ${cluster}, ${hTableName}, ${preSplitSize}, ${hTableTTL})""".execute.apply()
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val service = findById(id)
    val serviceName = service.serviceName
    sql"""delete from service_columns where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"serviceName=$serviceName")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
  }

  def findOrInsert(serviceName: String, cluster: String, hTableName: String,
                   preSplitSize: Int, hTableTTL: Option[Int], compressionAlgorithm: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Service = {
    findByName(serviceName, useCache) match {
      case Some(s) => s
      case None =>
        insert(serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)
        val cacheKey = "serviceName=" + serviceName
        expireCache(cacheKey)
        findByName(serviceName).get
    }
  }

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from services""".map { rs => Service(rs) }.list.apply
    putsToCache(ls.map { x =>
      val cacheKey = s"id=${x.id.get}"
      (cacheKey -> x)
    })

    putsToCache(ls.map { x =>
      val cacheKey = s"serviceName=${x.serviceName}"
      (cacheKey -> x)
    })
    ls
  }

  def findAllConn()(implicit session: DBSession = AutoSession): List[String] = {
    sql"""select distinct(cluster) from services""".map { rs => rs.string("cluster") }.list.apply
  }
}

case class Service(id: Option[Int],
                   serviceName: String,
                   accessToken: String,
                   cluster: String,
                   hTableName: String,
                   preSplitSize: Int,
                   hTableTTL: Option[Int],
                   options: Option[String] = None) {
  lazy val toJson =
    id match {
      case Some(_id) =>
        Json.obj("id" -> _id, "name" -> serviceName, "accessToken" -> accessToken, "cluster" -> cluster,
          "hTableName" -> hTableName, "preSplitSize" -> preSplitSize, "hTableTTL" -> hTableTTL)
      case None =>
        Json.parse("{}")
    }

  lazy val extraOptions = Model.extraOptions(options)
  lazy val storageConfigOpt: Option[Config] = toStorageConfig
  def toStorageConfig: Option[Config] = Model.toStorageConfig(extraOptions)
}
