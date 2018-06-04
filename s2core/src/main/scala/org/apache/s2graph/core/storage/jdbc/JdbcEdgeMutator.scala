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
import org.apache.s2graph.core.utils.logger
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

  def extractValues(affectedColumns: Seq[String], edge: S2EdgeLike): Seq[Any] = {
    val values = affectedColumns.collect {
      case "_timestamp" => new DateTime(edge.ts)
      case "_from" => edge.srcForVertex.innerId.value
      case "_to" => edge.tgtForVertex.innerId.value
      case k: String => edge.propertyValue(k).map(iv => iv.innerVal.value).orNull
    }

    values
  }

  def keyColumns(label: Label): Seq[String] =
    if (label.consistencyLevel == "strong") Seq("_from", "_to")
    else Seq("_from", "_to", "_timestamp")

  def insertOp(label: Label, edges: Seq[S2EdgeLike]): Seq[Boolean] = {
    val table = JdbcStorage.labelTableName(label)
    val affectedColumns = JdbcStorage.affectedColumns(label)
    val insertValues = edges.map { edge => extractValues(affectedColumns, edge) }

    val columns = affectedColumns.mkString(", ")
    val prepared = affectedColumns.map(_ => "?").mkString(", ")

    val conflictCheckKeys = keyColumns(label).mkString("(", ",", ")")

    val sqlRaw = s"""MERGE INTO ${table} (${columns}) KEY ${conflictCheckKeys} VALUES (${prepared});"""
    logger.debug(sqlRaw)

    val sql = SQLSyntax.createUnsafely(sqlRaw)
    val ret = withTxSession { session =>
      sql"""${sql}""".batch(insertValues: _*).apply()(session)
    }

    ret.map(_ => true)
  }

  def deleteOp(label: Label, edges: Seq[S2EdgeLike]): Seq[Boolean] = {
    val table = JdbcStorage.labelTableName(label)

    val keyColumnLs = keyColumns(label)
    val deleteValues = edges.map { edge => extractValues(keyColumnLs, edge) }
    val prepared = keyColumnLs.map(k => s"${k} = ?").mkString(" AND ")

    val sqlRaw = s"""DELETE FROM ${table} WHERE ($prepared);"""
    logger.debug(sqlRaw)

    val ret = withTxSession { session =>
      val sql = SQLSyntax.createUnsafely(sqlRaw)
      deleteValues.map { deleteValue =>
        sql"""${sql}""".batch(Seq(deleteValue): _*).apply()(session)
      }
    }

    ret.map(_ => true)
  }

  override def mutateStrongEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    val ret = _edges.groupBy(_.innerLabel).flatMap { case (label, edges) =>
      val (edgesToDelete, edgesToInsert) = edges.partition(_.getOperation() == "delete")

      insertOp(label, edgesToInsert) ++ deleteOp(label, edgesToDelete)
    }

    Future.successful(ret.toSeq)
  }

  override def mutateWeakEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] = {
    val ret = mutateStrongEdges(zkQuorum, _edges, withWait).map { ret =>
      ret.zipWithIndex.map { case (r, i) => i -> r }
    }

    ret
  }

  override def incrementCounts(zkQuorum: String, edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[MutateResponse]] = ???

  override def updateDegree(zkQuorum: String, edge: S2EdgeLike, degreeVal: Long)(implicit ec: ExecutionContext): Future[MutateResponse] = ???

  override def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult, requestTs: Long, retryNum: Int)(implicit ec: ExecutionContext): Future[Boolean] = ???
}
