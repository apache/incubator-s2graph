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
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.{Storage, StorageManagement, StorageSerDe}
import org.apache.s2graph.core.utils.logger
import scalikejdbc._

object JdbcStorage {

  case class Row(row: Map[String, Any])

  object Row {
    def apply(rs: WrappedResultSet): Row = Row(rs.toMap)
  }

  val PoolName = "_META_JDBC_STORAGE_"
  val TablePrefix = "_EDGE_STORE"

  def toMySqlType(tpe: String): String = tpe match {
    case "int" | "integer" | "i" | "int32" | "integer32" => "int(32)"
    case "string" | "str" | "s" => "varchar(256)"
    case "boolean" | "bool" => "tinyint(1)"
    case "long" | "l" | "int64" | "integer64" => "int(64)"
    case "float64" | "float" | "f" | "float32" => "float"
    case "double" | "d" => "double"
    case _ => ""
  }

  def buildIndex(label: Label, indices: List[LabelIndex]): String = {
    if (indices.isEmpty) s"KEY `${label.label}_PK` (`_timestamp`)" + ","
    else {
      val nameWithFields = indices.map { index =>
        val name = index.name
        val fields = index.propNames

        name -> fields.toList.map(n => s"`${n}`")
      }

      val ret = nameWithFields.map { case (name, fields) =>
        s"KEY `${label.label}_${name}` (${fields.mkString(", ")})"
      }

      ret.mkString(",\n") + ","
    }
  }

  def buildProps(metas: List[LabelMeta]): String = {
    if (metas.isEmpty) ""
    else {
      val ret = metas.map { meta =>
        s"""`${meta.name}` ${toMySqlType(meta.dataType)}""".trim
      }

      (ret.mkString(",\n  ") + ",").trim
    }
  }

  def showSchema(label: Label): String = {
    val srcColumn = label.srcColumn
    val tgtColumn = label.srcColumn
    val metas = LabelMeta.findAllByLabelId(label.id.get, useCache = false).sortBy(_.name)
    val consistency = label.consistencyLevel
    val indices = label.indices
    val isUnique = if (consistency == "strong") "UNIQUE" else ""

    s"""
       |CREATE TABLE `${labelTableName(label)}`(
       |  `id` int(11) NOT NULL AUTO_INCREMENT,
       |  `_timestamp` TIMESTAMP NOT NULL default CURRENT_TIMESTAMP,
       |  `_from` ${toMySqlType(srcColumn.columnType)} NOT NULL,
       |  `_to` ${toMySqlType(tgtColumn.columnType)} NOT NULL,
       |  PRIMARY KEY (`id`),
       |  ${buildProps(metas)}
       |  ${buildIndex(label, indices)}
       |  ${isUnique} KEY `${label.label}_from` (`_from`,`_to`),
       |  ${isUnique} KEY `${label.label}_to` (`_to`,`_from`)
       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  """.trim.stripMargin
  }

  def labelTableName(label: Label): String = s"${TablePrefix}_${label.label}"

  def affectedColumns(label: Label): Seq[String] = {
    Seq("_timestamp", "_from", "_to") ++ LabelMeta.findAllByLabelId(label.id.get, useCache = false).sortBy(_.name).map(_.name)
  }

  def withTxSession[T](f: => DBSession => T): T = {
    using(DB(ConnectionPool.borrow(PoolName))) { db =>
      db localTx { implicit session =>
        f(session)
      }
    }
  }

  def withReadOnlySession[T](f: => DBSession => T): T = {
    using(DB(ConnectionPool.borrow(PoolName))) { db =>
      f(db.readOnlySession())
    }
  }

  def dropTable(label: Label): Unit = {
    val cmd = s"DROP TABLE IF EXISTS ${labelTableName(label)}"

    withTxSession { session =>
      sql"${SQLSyntax.createUnsafely(cmd)}".execute().apply()(session)
    }
  }

  def tables = DB(ConnectionPool.borrow(PoolName)).getTableNames().map(_.toLowerCase())

  def createTable(label: Label): Boolean = {
    val schema = showSchema(label)
    val tableName = labelTableName(label)

    if (tables.contains(tableName.toLowerCase())) {
      logger.info(s"Table exists ${tableName}")
    } else {
      withTxSession { session =>
        sql"${SQLSyntax.createUnsafely(schema)}".execute().apply()(session)
      }
    }

    true
  }

  def prepareConnection(config: Config): Unit = {
    val jdbcDriverName = config.getString("driver")
    Class.forName(jdbcDriverName)

    if (!ConnectionPool.isInitialized(PoolName)) {
      val jdbcUrl = config.getString("url")
      val user = config.getString("user")
      val password = config.getString("password")
      val settings = ConnectionPoolSettings(initialSize = 10, maxSize = 10, connectionTimeoutMillis = 30000L, validationQuery = "select 1;")

      ConnectionPool.add(PoolName, jdbcUrl, user, password, settings)
    }
  }
}

class JdbcStorage(graph: S2GraphLike, config: Config) {

  implicit val ec = graph.ec

  JdbcStorage.prepareConnection(config)

  val h2EdgeFetcher = new JdbcEdgeFetcher(graph)
  h2EdgeFetcher.init(config)

  val h2EdgeMutator = new JdbcEdgeMutator(graph)
  h2EdgeMutator.init(config)

  val edgeFetcher: EdgeFetcher = h2EdgeFetcher
  val edgeMutator: EdgeMutator = h2EdgeMutator
}

