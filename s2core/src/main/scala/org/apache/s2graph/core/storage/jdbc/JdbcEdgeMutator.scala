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
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.storage.MutateResponse
import org.joda.time.DateTime
import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}

class JdbcEdgeMutator(graph: S2GraphLike) extends EdgeMutator {

  import JdbcStorage._

  override def init(config: Config)(implicit ec: ExecutionContext): Unit = {
    val labelName = config.getString("labelName")
    val label = Label.findByName(labelName, useCache = false).get

    JdbcStorage.prepareConnection(config)
    createTable(label)
  }

  override def mutateStrongEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    _edges.groupBy(_.innerLabel).flatMap { case (label, edges) =>
      val affectedColumns = JdbcStorage.affectedColumns(label)

      val insertValues = edges.map { edge =>
        val values = affectedColumns.collect {
          case "_timestamp" => new DateTime(edge.ts)
          case "_from" => edge.srcForVertex.innerId.value
          case "_to" => edge.tgtForVertex.innerId.value
          case k: String => edge.propertyValue(k).map(iv => iv.innerVal.value).orNull
        }

        values
      }

      val columns = affectedColumns.mkString(", ")
      val table = JdbcStorage.labelTableName(label)
      val prepared = affectedColumns.map(_ => "?").mkString(", ")

      val conflictCheckKeys =
        if (label.consistencyLevel == "strong") "(_from, _to)"
        else "(_from, _to, _timestamp)"

      val sqlRaw =
        s"""MERGE INTO ${table} (${columns}) KEY ${conflictCheckKeys} VALUES (${prepared})""".stripMargin

      val sql = SQLSyntax.createUnsafely(sqlRaw)
      withTxSession { session =>
        sql"""${sql}""".batch(insertValues: _*).apply()(session)
      }

      insertValues
    }

    Future.successful(_edges.map(_ => true))
  }

  override def mutateWeakEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] = ???

  override def incrementCounts(zkQuorum: String, edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[MutateResponse]] = ???

  override def updateDegree(zkQuorum: String, edge: S2EdgeLike, degreeVal: Long)(implicit ec: ExecutionContext): Future[MutateResponse] = ???

  override def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult, requestTs: Long, retryNum: Int)(implicit ec: ExecutionContext): Future[Boolean] = ???
}
