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

package org.apache.s2graph.s2jobs

import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.types.{InnerValLikeWithTs, SourceVertexId}
import org.apache.s2graph.s2jobs.loader.GraphFileOptions
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.ExecutionContext
import scala.util.Try

object S2GraphHelper {
  def initS2Graph(config: Config)(implicit ec: ExecutionContext = ExecutionContext.Implicits.global): S2Graph = {
    new S2Graph(config)
  }

  def buildDegreePutRequests(s2: S2Graph,
                             vertexId: String,
                             labelName: String,
                             direction: String,
                             degreeVal: Long): Seq[SKeyValue] = {
    val label = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"$labelName is not found in DB."))
    val dir = GraphUtil.directions(direction)
    val innerVal = JSONParser.jsValueToInnerVal(Json.toJson(vertexId), label.srcColumnWithDir(dir).columnType, label.schemaVersion).getOrElse {
      throw new RuntimeException(s"$vertexId can not be converted into innerval")
    }
    val vertex = s2.elementBuilder.newVertex(SourceVertexId(label.srcColumn, innerVal))

    val ts = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion))
    val edge = s2.elementBuilder.newEdge(vertex, vertex, label, dir, propsWithTs = propsWithTs)

    edge.edgesWithIndex.flatMap { indexEdge =>
      s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues
    }
  }

  private def insertBulkForLoaderAsync(s2: S2Graph, edge: S2Edge, createRelEdges: Boolean = true): Seq[SKeyValue] = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)

    val snapshotEdgeKeyValues = s2.getStorage(edge.toSnapshotEdge.label).serDe.snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues
    val indexEdgeKeyValues = relEdges.flatMap { edge =>
      edge.edgesWithIndex.flatMap { indexEdge =>
        s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues
      }
    }

    snapshotEdgeKeyValues ++ indexEdgeKeyValues
  }

  def toSKeyValues(s2: S2Graph, element: GraphElement, autoEdgeCreate: Boolean = false): Seq[SKeyValue] = {
    if (element.isInstanceOf[S2Edge]) {
      val edge = element.asInstanceOf[S2Edge]
      insertBulkForLoaderAsync(s2, edge, autoEdgeCreate)
    } else if (element.isInstanceOf[S2Vertex]) {
      val vertex = element.asInstanceOf[S2Vertex]
      s2.getStorage(vertex.service).serDe.vertexSerializer(vertex).toKeyValues
    } else {
      Nil
    }
  }

  //TODO:
  def toGraphFileOptions(taskConf: TaskConf): GraphFileOptions = {
    GraphFileOptions()
  }

  def sparkSqlRowToGraphElement(s2: S2Graph, row: Row, schema: StructType, reservedColumn: Set[String]): Option[GraphElement] = {
    val timestamp = row.getAs[Long]("timestamp")
    val operation = Try(row.getAs[String]("operation")).getOrElse("insert")
    val elem = Try(row.getAs[String]("elem")).getOrElse("e")

    val props: Map[String, Any] = Option(row.getAs[String]("props")) match {
      case Some(propsStr:String) =>
        JSONParser.fromJsonToProperties(Json.parse(propsStr).as[JsObject])
      case None =>
        schema.fieldNames.flatMap { field =>
          if (!reservedColumn.contains(field)) {
            Seq(
              field -> getRowValAny(row, field)
            )
          } else Nil
        }.toMap
    }

    elem match {
      case "e" | "edge" =>
        val from = getRowValAny(row, "from")
        val to = getRowValAny(row, "to")
        val label = row.getAs[String]("label")
        val direction = Try(row.getAs[String]("direction")).getOrElse("out")
        Some(
          s2.elementBuilder.toEdge(from, to, label, direction, props, timestamp, operation)
        )
      case "v" | "vertex" =>
        val id = getRowValAny(row, "id")
        val serviceName = row.getAs[String]("service")
        val columnName = row.getAs[String]("column")
        Some(
          s2.elementBuilder.toVertex(serviceName, columnName, id, props, timestamp, operation)
        )
      case _ =>
        None
    }
  }

  private def getRowValAny(row:Row, fieldName:String):Any = {
    val idx = row.fieldIndex(fieldName)
    row.get(idx)
  }
}
