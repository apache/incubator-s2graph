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

package org.apache.s2graph.core

import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.JSONParser.{fromJsonToProperties, toInnerVal}
import org.apache.s2graph.core.S2Graph.{DefaultColumnName, DefaultServiceName}
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure.T
import play.api.libs.json.{JsObject, Json}

import scala.util.Try

class GraphElementBuilder(graph: S2GraphLike) {

  def toGraphElement(s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] = Try {
    val parts = GraphUtil.split(s)
    val logType = parts(2)
    val element = if (logType == "edge" | logType == "e") {
      /* current only edge is considered to be bulk loaded */
      labelMapping.get(parts(5)) match {
        case None =>
        case Some(toReplace) =>
          parts(5) = toReplace
      }
      toEdge(parts)
    } else if (logType == "vertex" | logType == "v") {
      toVertex(parts)
    } else {
      throw new GraphExceptions.JsonParseException("log type is not exist in log.")
    }

    element
  } recover {
    case e: Exception =>
      logger.error(s"[toElement]: $s", e)
      None
  } get


  def toVertex(s: String): Option[S2VertexLike] = {
    toVertex(GraphUtil.split(s))
  }

  def toEdge(s: String): Option[S2EdgeLike] = {
    toEdge(GraphUtil.split(s))
  }

  def toEdge(parts: Array[String]): Option[S2EdgeLike] = Try {
    val (ts, operation, logType, srcId, tgtId, label) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val tempDirection = if (parts.length >= 8) parts(7) else "out"
    val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection
    val edge = toEdge(srcId, tgtId, label, direction, props, ts.toLong, operation)
    Option(edge)
  } recover {
    case e: Exception =>
      logger.error(s"[toEdge]: $e", e)
      throw e
  } get

  def toVertex(parts: Array[String]): Option[S2VertexLike] = Try {
    val (ts, operation, logType, srcId, serviceName, colName) = (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
    val props = if (parts.length >= 7) fromJsonToProperties(Json.parse(parts(6)).asOpt[JsObject].getOrElse(Json.obj())) else Map.empty[String, Any]
    val vertex = toVertex(serviceName, colName, srcId, props, ts.toLong, operation)
    Option(vertex)
  } recover {
    case e: Throwable =>
      logger.error(s"[toVertex]: $e", e)
      throw e
  } get

  def toEdge(srcId: Any,
             tgtId: Any,
             labelName: String,
             direction: String,
             props: Map[String, Any] = Map.empty,
             ts: Long = System.currentTimeMillis(),
             operation: String = "insert"): S2EdgeLike = {
    val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))

    val srcColumn = if (direction == "out") label.srcColumn else label.tgtColumn
    val tgtColumn = if (direction == "out") label.tgtColumn else label.srcColumn

    val srcVertexIdInnerVal = toInnerVal(srcId, srcColumn.columnType, label.schemaVersion)
    val tgtVertexIdInnerVal = toInnerVal(tgtId, tgtColumn.columnType, label.schemaVersion)

    val srcVertex = newVertex(new SourceVertexId(srcColumn, srcVertexIdInnerVal), System.currentTimeMillis())
    val tgtVertex = newVertex(new TargetVertexId(tgtColumn, tgtVertexIdInnerVal), System.currentTimeMillis())
    val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))

    val propsPlusTs = props ++ Map(LabelMeta.timestamp.name -> ts)
    val propsWithTs = label.propsToInnerValsWithTs(propsPlusTs, ts)
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    new S2Edge(graph, srcVertex, tgtVertex, label, dir, op = op, version = ts).copyEdgeWithState(propsWithTs)
  }

  def toVertex(serviceName: String,
               columnName: String,
               id: Any,
               props: Map[String, Any] = Map.empty,
               ts: Long = System.currentTimeMillis(),
               operation: String = "insert"): S2VertexLike = {

    val service = Service.findByName(serviceName).getOrElse(throw new java.lang.IllegalArgumentException(s"$serviceName is not found."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new java.lang.IllegalArgumentException(s"$columnName is not found."))
    val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

    val srcVertexId = id match {
      case vid: VertexId => id.asInstanceOf[VertexId]
      case _ => new VertexId(column, toInnerVal(id, column.columnType, column.schemaVersion))
    }

    val propsInner = column.propsToInnerVals(props) ++
      Map(ColumnMeta.timestamp -> InnerVal.withLong(ts, column.schemaVersion))

    val vertex = new S2Vertex(graph, srcVertexId, ts, S2Vertex.EmptyProps, op)
    S2Vertex.fillPropsWithTs(vertex, propsInner)
    vertex
  }


  /**
    * helper to create new Edge instance from given parameters on memory(not actually stored in storage).
    *
    * Since we are using mutable map for property value(propsWithTs),
    * we should make sure that reference for mutable map never be shared between multiple Edge instances.
    * To guarantee this, we never create Edge directly, but rather use this helper which is available on S2Graph.
    *
    * Note that we are using following convention
    * 1. `add*` for method that actually store instance into storage,
    * 2. `new*` for method that only create instance on memory, but not store it into storage.
    *
    * @param srcVertex
    * @param tgtVertex
    * @param innerLabel
    * @param dir
    * @param op
    * @param version
    * @param propsWithTs
    * @param parentEdges
    * @param originalEdgeOpt
    * @param pendingEdgeOpt
    * @param statusCode
    * @param lockTs
    * @param tsInnerValOpt
    * @return
    */
  def newEdge(srcVertex: S2VertexLike,
              tgtVertex: S2VertexLike,
              innerLabel: Label,
              dir: Int,
              op: Byte = GraphUtil.defaultOpByte,
              version: Long = System.currentTimeMillis(),
              propsWithTs: S2Edge.State,
              parentEdges: Seq[EdgeWithScore] = Nil,
              originalEdgeOpt: Option[S2EdgeLike] = None,
              pendingEdgeOpt: Option[S2EdgeLike] = None,
              statusCode: Byte = 0,
              lockTs: Option[Long] = None,
              tsInnerValOpt: Option[InnerValLike] = None): S2EdgeLike = {
    val edge = S2Edge(
      graph,
      srcVertex,
      tgtVertex,
      innerLabel,
      dir,
      op,
      version,
      S2Edge.EmptyProps,
      parentEdges,
      originalEdgeOpt,
      pendingEdgeOpt,
      statusCode,
      lockTs,
      tsInnerValOpt)
    S2Edge.fillPropsWithTs(edge, propsWithTs)
    edge
  }

  /**
    * helper to create new SnapshotEdge instance from given parameters on memory(not actually stored in storage).
    *
    * Note that this is only available to S2Graph, not structure.Graph so only internal code should use this method.
    * @param srcVertex
    * @param tgtVertex
    * @param label
    * @param dir
    * @param op
    * @param version
    * @param propsWithTs
    * @param pendingEdgeOpt
    * @param statusCode
    * @param lockTs
    * @param tsInnerValOpt
    * @return
    */
  private[core] def newSnapshotEdge(srcVertex: S2VertexLike,
                                    tgtVertex: S2VertexLike,
                                    label: Label,
                                    dir: Int,
                                    op: Byte,
                                    version: Long,
                                    propsWithTs: S2Edge.State,
                                    pendingEdgeOpt: Option[S2EdgeLike],
                                    statusCode: Byte = 0,
                                    lockTs: Option[Long],
                                    tsInnerValOpt: Option[InnerValLike] = None): SnapshotEdge = {
    val snapshotEdge = new SnapshotEdge(graph, srcVertex, tgtVertex, label, dir, op, version, S2Edge.EmptyProps,
      pendingEdgeOpt, statusCode, lockTs, tsInnerValOpt)
    S2Edge.fillPropsWithTs(snapshotEdge, propsWithTs)
    snapshotEdge
  }

  def newVertexId(serviceName: String)(columnName: String)(id: Any): VertexId = {
    val service = Service.findByName(serviceName).getOrElse(throw new RuntimeException(s"$serviceName is not found."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new RuntimeException(s"$columnName is not found."))
    newVertexId(service, column, id)
  }

  /**
    * helper to create S2Graph's internal S2VertexId instance with given parameters.
    * @param service
    * @param column
    * @param id
    * @return
    */
  def newVertexId(service: Service,
                  column: ServiceColumn,
                  id: Any): VertexId = {
    val innerVal = CanInnerValLike.anyToInnerValLike.toInnerVal(id)(column.schemaVersion)
    new VertexId(column, innerVal)
  }

  def newVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2VertexLike = {
    val vertex = new S2Vertex(graph, id, ts, S2Vertex.EmptyProps, op, belongLabelIds)
    S2Vertex.fillPropsWithTs(vertex, props)
    vertex
  }

  def makeVertex(idValue: AnyRef, kvsMap: Map[String, AnyRef]): S2VertexLike = {
    idValue match {
      case vId: VertexId =>
        toVertex(vId.column.service.serviceName, vId.column.columnName, vId, kvsMap)
      case _ =>
        val serviceColumnNames = kvsMap.getOrElse(T.label.toString, DefaultColumnName).toString

        val names = serviceColumnNames.split(S2Vertex.VertexLabelDelimiter)
        val (serviceName, columnName) =
          if (names.length == 1) (DefaultServiceName, names(0))
          else throw new RuntimeException("malformed data on vertex label.")

        toVertex(serviceName, columnName, idValue, kvsMap)
    }
  }

  def toRequestEdge(queryRequest: QueryRequest, parentEdges: Seq[EdgeWithScore]): S2EdgeLike = {
    val srcVertex = queryRequest.vertex
    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val propsWithTs = label.EmptyPropsWithTs

    tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /* we use toSnapshotEdge so dont need to swap src, tgt */
        val src = srcVertex.innerId
        val tgt = tgtVertexId
        val (srcVId, tgtVId) = (new SourceVertexId(srcColumn, src), new TargetVertexId(tgtColumn, tgt))
        val (srcV, tgtV) = (newVertex(srcVId), newVertex(tgtVId))

        newEdge(srcV, tgtV, label, labelWithDir.dir, propsWithTs = propsWithTs)
      case None =>
        val src = srcVertex.innerId
        val srcVId = new SourceVertexId(srcColumn, src)
        val srcV = newVertex(srcVId)

        newEdge(srcV, srcV, label, labelWithDir.dir, propsWithTs = propsWithTs, parentEdges = parentEdges)
    }
  }

  def buildEdgesToDelete(edgeWithScoreLs: Seq[EdgeWithScore], requestTs: Long): Seq[EdgeWithScore] = {
    if (edgeWithScoreLs.isEmpty) Nil
    else {
      val head = edgeWithScoreLs.head
      val label = head.edge.innerLabel

      //Degree edge?
      edgeWithScoreLs.map { case edgeWithScore =>
        val edge = edgeWithScore.edge
        val copiedEdge = label.consistencyLevel match {
          case "strong" =>
            edge
              .copyEdgeWithState(S2Edge.propsToState(edge.updatePropsWithTs()))
              .copyTs(requestTs)
              .copyOp(GraphUtil.operations("delete"))
              .copyVersion(requestTs)
          case _ =>
            edge
              .copyEdgeWithState(S2Edge.propsToState(edge.updatePropsWithTs()))
              .copyTs(requestTs)
        }

        val edgeToDelete = edgeWithScore.copy(edge = copiedEdge)
        //      logger.debug(s"delete edge from deleteAll: ${edgeToDelete.edge.toLogString}")
        edgeToDelete
      }
    }
  }
}
