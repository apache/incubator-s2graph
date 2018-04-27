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

import play.api.libs.json.{JsValue, Json}
import scalikejdbc._

import scala.util.Try

object Bucket extends SQLSyntaxSupport[Bucket] {
  import Schema._
  val className = Bucket.getClass.getSimpleName

  val rangeDelimiter = "~"
  val INVALID_BUCKET_EXCEPTION = new RuntimeException("invalid bucket.")
  val InActiveModulars = Set("0~0")

  def valueOf(rs: WrappedResultSet): Bucket = {
    Bucket(rs.intOpt("id"),
      rs.int("experiment_id"),
      rs.string("modular"),
      rs.string("http_verb"),
      rs.string("api_path"),
      rs.string("request_body"),
      rs.int("timeout"),
      rs.string("impression_id"),
      rs.boolean("is_graph_query"),
      rs.boolean("is_empty"))
  }

  def finds(experimentId: Int)(implicit session: DBSession = AutoSession): List[Bucket] = {
    val cacheKey = className + "experimentId=" + experimentId

    withCaches(cacheKey, broadcast = false) {
      sql"""select * from buckets where experiment_id = $experimentId"""
        .map { rs => Bucket.valueOf(rs) }.list().apply()
    }
  }

  def toRange(str: String): Option[(Int, Int)] = {
    val range = str.split(rangeDelimiter)
    if (range.length == 2) Option((range.head.toInt, range.last.toInt))
    else None
  }

  def findByImpressionId(impressionId: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[Bucket] = {
    val cacheKey = className + "impressionId=" + impressionId

    lazy val sql = sql"""select * from buckets where impression_id=$impressionId"""
      .map { rs => Bucket.valueOf(rs)}.single().apply()

    if (useCache) withCache(cacheKey)(sql)
    else sql

  }

  def findById(id: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Bucket = {
    val cacheKey = className + "id=" + id
    lazy val sql = sql"""select * from buckets where id = $id""".map { rs => Bucket.valueOf(rs)}.single().apply()
    if (useCache) withCache(cacheKey, false) { sql }.get
    else sql.get
  }

  def update(id: Int,
             experimentId: Int,
             modular: String,
             httpVerb: String,
             apiPath: String,
             requestBody: String,
             timeout: Int,
             impressionId: String,
             isGraphQuery: Boolean,
             isEmpty: Boolean)(implicit session: DBSession = AutoSession): Try[Bucket] = {
    Try {
      sql"""
            UPDATE buckets set experiment_id = $experimentId, modular = $modular, http_verb = $httpVerb, api_path = $apiPath,
            request_body = $requestBody, timeout = $timeout, impression_id = $impressionId,
            is_graph_query = $isGraphQuery, is_empty = $isEmpty WHERE id = $id
        """
        .update().apply()
    }.map { cnt =>
      findById(id)
    }
  }

  def insert(experimentId: Int, modular: String, httpVerb: String, apiPath: String,
             requestBody: String, timeout: Int, impressionId: String,
             isGraphQuery: Boolean, isEmpty: Boolean)
            (implicit session: DBSession = AutoSession): Try[Bucket] = {
    Try {
      sql"""
            INSERT INTO buckets(experiment_id, modular, http_verb, api_path, request_body, timeout, impression_id,
             is_graph_query, is_empty)
            VALUES (${experimentId}, $modular, $httpVerb, $apiPath, $requestBody, $timeout, $impressionId,
             $isGraphQuery, $isEmpty)
        """
        .updateAndReturnGeneratedKey().apply()
    }.map { newId =>
      Bucket(Some(newId.toInt), experimentId, modular, httpVerb, apiPath, requestBody, timeout, impressionId,
        isGraphQuery, isEmpty)
    }
  }
}

case class Bucket(id: Option[Int],
                  experimentId: Int,
                  modular: String,
                  httpVerb: String, apiPath: String,
                  requestBody: String, timeout: Int, impressionId: String,
                  isGraphQuery: Boolean = true,
                  isEmpty: Boolean = false) {

  import Bucket._

  lazy val rangeOpt = toRange(modular)

  def toJson(): JsValue =
    Json.obj("id" -> id, "experimentId" -> experimentId, "modular" -> modular, "httpVerb" -> httpVerb,
      "requestBody" -> requestBody, "isGraphQuery" -> isGraphQuery, "isEmpty" -> isEmpty)

}
