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

import com.sksamuel.elastic4s.{ElasticsearchClientUri, IndexAndType}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.typesafe.config.Config
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.{EdgeId, S2EdgeLike, S2VertexLike}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class ESIndexProvider(config: Config)(implicit ec: ExecutionContext) extends IndexProvider {
  import GlobalIndex._
  import IndexProvider._

  import scala.collection.mutable

  implicit val executor = ec

  val esClientUri = Try(config.getString("es.index.provider.client.uri")).getOrElse("localhost")
  val client = HttpClient(ElasticsearchClientUri(esClientUri, 9200))

  val WaitTime = Duration("60 seconds")

  private def toFields(vertex: S2VertexLike, forceToIndex: Boolean): Option[Map[String, Any]] = {
    val props = vertex.props.asScala
    val storeInGlobalIndex = if (forceToIndex) true else props.exists(_._2.columnMeta.storeInGlobalIndex)

    if (!storeInGlobalIndex) None
    else {
      val fields = mutable.Map.empty[String, Any]

      fields += (vidField -> vertex.id.toString())
      fields += (serviceField -> vertex.serviceName)
      fields += (serviceColumnField -> vertex.columnName)

      props.foreach { case (dim, s2VertexProperty) =>
        // skip reserved fields.
        if (s2VertexProperty.columnMeta.seq > 0) {
          s2VertexProperty.columnMeta.dataType match {
            case "string" => fields += (dim -> s2VertexProperty.innerVal.value.toString)
            case _ => fields += (dim -> s2VertexProperty.innerVal.value)
          }
        }
      }

      Option(fields.toMap)
    }
  }

  private def toFields(edge: S2EdgeLike, forceToIndex: Boolean): Option[Map[String, Any]] = {
    val props = edge.getPropsWithTs().asScala
    val store = if (forceToIndex) true else props.exists(_._2.labelMeta.storeInGlobalIndex)

    if (!store) None
    else {
      val fields = mutable.Map.empty[String, Any]

      fields += (eidField -> edge.edgeId.toString)
      fields += (serviceField -> edge.serviceName)
      fields += (labelField -> edge.label())

      props.foreach { case (dim, s2Property) =>
        if (s2Property.labelMeta.seq > 0) {
          s2Property.labelMeta.dataType match {
            case "string" => fields += (dim -> s2Property.innerVal.value.toString)
            case _ => fields += (dim -> s2Property.innerVal.value)
          }
        }
      }

      Option(fields.toMap)
    }
  }

  override def mutateVerticesAsync(vertices: Seq[S2VertexLike], forceToIndex: Boolean = false): Future[Seq[Boolean]] = {
    val bulkRequests = vertices.flatMap { vertex =>
        toFields(vertex, forceToIndex).toSeq.map { fields =>
          update(vertex.id.toString()).in(new IndexAndType(GlobalIndex.VertexIndexName, GlobalIndex.TypeName)).docAsUpsert(fields)
        }
      }

    if (bulkRequests.isEmpty) Future.successful(vertices.map(_ => true))
    else {
      client.execute {
        val requests = bulk(requests = bulkRequests)

        requests
      }.map { ret =>
        ret match {
          case Left(failure) => vertices.map(_ => false)
          case Right(results) => vertices.map(_ => true)
        }
      }
    }
  }

  override def mutateVertices(vertices: Seq[S2VertexLike], forceToIndex: Boolean = false): Seq[Boolean] =
    Await.result(mutateVerticesAsync(vertices, forceToIndex), WaitTime)

  override def mutateEdges(edges: Seq[S2EdgeLike], forceToIndex: Boolean = false): Seq[Boolean] =
    Await.result(mutateEdgesAsync(edges, forceToIndex), WaitTime)

  override def mutateEdgesAsync(edges: Seq[S2EdgeLike], forceToIndex: Boolean = false): Future[Seq[Boolean]] = {
    val bulkRequests = edges.flatMap { edge =>
      toFields(edge, forceToIndex).toSeq.map { fields =>
        update(edge.edgeId.toString()).in(new IndexAndType(GlobalIndex.EdgeIndexName, GlobalIndex.TypeName)).docAsUpsert(fields)
      }
    }

    if (bulkRequests.isEmpty) Future.successful(edges.map(_ => true))
    else {
      client.execute {
        bulk(bulkRequests)
      }.map { ret =>
        ret match {
          case Left(failure) => edges.map(_ => false)
          case Right(results) => edges.map(_ => true)
        }
      }
    }
  }

  override def fetchEdgeIds(hasContainers: util.List[HasContainer]): util.List[EdgeId] =
    Await.result(fetchEdgeIdsAsync(hasContainers), WaitTime)

  override def fetchEdgeIdsAsync(hasContainers: util.List[HasContainer]): Future[util.List[EdgeId]] = {
    val field = eidField
    val ids = new java.util.HashSet[EdgeId]

    val queryString = buildQueryString(hasContainers)

    client.execute {
      search(GlobalIndex.EdgeIndexName).query(queryString)
    }.map { ret =>
      ret match {
        case Left(failure) =>
        case Right(results) =>
          results.result.hits.hits.foreach { searchHit =>
            searchHit.sourceAsMap.get(field).foreach { idValue =>
              val id = Conversions.s2EdgeIdReads.reads(Json.parse(idValue.toString)).get

              //TODO: Come up with better way to filter out hits with invalid meta.
              EdgeId.isValid(id).foreach(ids.add)
            }
          }
      }

      new util.ArrayList[EdgeId](ids)
    }
  }


  override def fetchVertexIds(hasContainers: util.List[HasContainer]): util.List[VertexId] =
    Await.result(fetchVertexIdsAsync(hasContainers), WaitTime)

  override def fetchVertexIdsAsync(hasContainers: util.List[HasContainer]): Future[util.List[VertexId]] = {
    val field = vidField
    val ids = new java.util.HashSet[VertexId]

    val queryString = buildQueryString(hasContainers)

    client.execute {
      search(GlobalIndex.VertexIndexName).query(queryString)
    }.map { ret =>
      ret match {
        case Left(failure) =>
        case Right(results) =>
          results.result.hits.hits.foreach { searchHit =>
            searchHit.sourceAsMap.get(field).foreach { idValue =>
              val id = Conversions.s2VertexIdReads.reads(Json.parse(idValue.toString)).get
              //TODO: Come up with better way to filter out hits with invalid meta.
              VertexId.isValid(id).foreach(ids.add)
            }
          }
      }

      new util.ArrayList[VertexId](ids)
    }
  }

  override def shutdown(): Unit = {
    client.close()
  }
}
