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

package org.apache.s2graph.core.index

import java.util

import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.VertexId
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.process.traversal.util.{AndP, OrP}
import org.apache.tinkerpop.gremlin.process.traversal.{Compare, Contains}
import org.apache.tinkerpop.gremlin.structure.T

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object IndexProvider {
  import GlobalIndex._
  val IdField = "id"

  def apply(config: Config)(implicit ec: ExecutionContext): IndexProvider = {

    val indexProviderType = Try { config.getString("index.provider") }.getOrElse("lucene")

    indexProviderType match {
      case "lucene" => new LuceneIndexProvider(config)
      case "es" => new ESIndexProvider(config)
    }
  }

  def buildQuerySingleString(container: HasContainer): String = {
    import scala.collection.JavaConversions._

    val key = if (container.getKey == T.label.getAccessor) labelField else container.getKey
    val value = container.getValue

    container.getPredicate match {
      case and: AndP[_] =>
        val buffer = scala.collection.mutable.ArrayBuffer.empty[String]
        and.getPredicates.foreach { p =>
          buffer.append(buildQuerySingleString(new HasContainer(container.getKey, p)))
        }
        buffer.mkString("(", " AND ", ")")
      case or: OrP[_] =>
        val buffer = scala.collection.mutable.ArrayBuffer.empty[String]
        or.getPredicates.foreach { p =>
          buffer.append(buildQuerySingleString(new HasContainer(container.getKey, p)))
        }
        buffer.mkString("(", " OR ", ")")
      case _ =>
        val biPredicate = container.getBiPredicate
        biPredicate match {

          case Contains.within =>
            key + ":(" + value.asInstanceOf[util.Collection[_]].toSeq.mkString(" OR ") + ")"
          case Contains.without =>
            "NOT " + key + ":(" + value.asInstanceOf[util.Collection[_]].toSeq.mkString(" AND ") + ")"
          case Compare.eq => s"${key}:${value}"
          case Compare.gt => s"(${key}:[${value} TO *] AND NOT ${key}:${value})"
          case Compare.gte => s"${key}:[${value} TO *]"
          case Compare.lt => s"${key}:[* TO ${value}]"
          case Compare.lte => s"(${key}:[* TO ${value}] OR ${key}:${value})"
          case Compare.neq => s"NOT ${key}:${value}"
          case _ => throw new IllegalArgumentException("not supported yet.")
        }
    }
  }

  def buildQueryString(hasContainers: java.util.List[HasContainer]): String = {
    import scala.collection.JavaConversions._
    val builder = scala.collection.mutable.ArrayBuffer.empty[String]

    hasContainers.foreach { container =>
      container.getPredicate match {
        case and: AndP[_] =>
          val buffer = scala.collection.mutable.ArrayBuffer.empty[String]
          and.getPredicates.foreach { p =>
            buffer.append(buildQuerySingleString(new HasContainer(container.getKey, p)))
          }
          builder.append(buffer.mkString("(", " AND ", ")"))
        case or: OrP[_] =>
          val buffer = scala.collection.mutable.ArrayBuffer.empty[String]
          or.getPredicates.foreach { p =>
            buffer.append(buildQuerySingleString(new HasContainer(container.getKey, p)))
          }
          builder.append(buffer.mkString("(", " OR ", ")"))
        case _ =>
          builder.append(buildQuerySingleString(container))
      }
    }

    builder.mkString(" AND ")
  }
}

trait IndexProvider {
  //TODO: Seq nee do be changed into stream
  def fetchEdgeIds(hasContainers: java.util.List[HasContainer]): java.util.List[EdgeId]
  def fetchEdgeIdsAsync(hasContainers: java.util.List[HasContainer]): Future[java.util.List[EdgeId]]

  def fetchVertexIds(hasContainers: java.util.List[HasContainer]): java.util.List[VertexId]
  def fetchVertexIdsAsync(hasContainers: java.util.List[HasContainer]): Future[java.util.List[VertexId]]
  def fetchVertexIdsAsyncRaw(vertexQueryParam: VertexQueryParam): Future[java.util.List[VertexId]] = Future.successful(util.Arrays.asList())

  def mutateVertices(vertices: Seq[S2VertexLike], forceToIndex: Boolean = false): Seq[Boolean]
  def mutateVerticesAsync(vertices: Seq[S2VertexLike], forceToIndex: Boolean = false): Future[Seq[Boolean]]

  def mutateEdges(edges: Seq[S2EdgeLike], forceToIndex: Boolean = false): Seq[Boolean]
  def mutateEdgesAsync(edges: Seq[S2EdgeLike], forceToIndex: Boolean = false): Future[Seq[Boolean]]

  def shutdown(): Unit
}
