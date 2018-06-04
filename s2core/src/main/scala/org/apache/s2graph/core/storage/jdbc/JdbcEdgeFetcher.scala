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

package org.apache.s2graph.core.storage.jdbc

import com.typesafe.config.Config
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.{Label, LabelMeta}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.{ExecutionContext, Future}
import scalikejdbc._

class JdbcEdgeFetcher(graph: S2GraphLike) extends EdgeFetcher {

  import JdbcStorage._

  var selectQuery: SQLSyntax = _

  override def init(config: Config)(implicit ec: ExecutionContext): Unit = {
    import JdbcStorage._

    val labelName = config.getString("labelName")
    val label = Label.findByName(labelName, useCache = false).get

    selectQuery = SQLSyntax.createUnsafely(
      s"SELECT ${affectedColumns(label).mkString(", ")} FROM ${labelTableName(label)}"
    )

    JdbcStorage.prepareConnection(config)
    createTable(label)
  }

  def toEdge(metas: Seq[LabelMeta], labelName: String, direction: String, rs: Map[String, Any]): S2EdgeLike = {
    val from = rs("_from".toUpperCase())
    val to = rs("_to".toUpperCase())
    val timestamp = rs("_timestamp".toUpperCase()).asInstanceOf[java.sql.Timestamp].millis

    val props = metas.map { meta =>
      meta.name -> rs(meta.name.toUpperCase())
    }.toMap

    if (direction == "out") {
      graph.toEdge(from, to, labelName, direction, props, timestamp)
    } else {
      graph.toEdge(to, from, labelName, direction, props, timestamp)
    }
  }

  override def fetches(queryRequests: Seq[QueryRequest], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    withReadOnlySession { implicit session =>
      val stepResultLs = queryRequests.map { queryRequest =>
        val vertex = queryRequest.vertex
        val queryParam = queryRequest.queryParam
        val label = queryParam.label
        val metas = label.metas()

        val limit = queryParam.limit
        val offset = queryParam.offset
        val cond = vertex.innerId.toIdString()
        val orderColumn = SQLSyntax.createUnsafely("_timestamp")

        val rows = if (queryParam.direction == "out") {
          sql"""${selectQuery} WHERE _from = ${cond} ORDER BY ${orderColumn} DESC offset ${offset} limit ${limit}""".map(Row.apply).list().apply()
        } else {
          sql"""${selectQuery} WHERE _to = ${cond} ORDER BY ${orderColumn} DESC offset ${offset} limit ${limit}""".map(Row.apply).list().apply()
        }

        val edgeWithScores = rows.map { row =>
          val edge = toEdge(metas, label.label, queryParam.direction, row.row)
          EdgeWithScore(edge, 1.0, queryParam.label)
        }

        StepResult(edgeWithScores, Nil, Nil)
      }

      Future.successful(stepResultLs)
    }
  }

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = ???
}
