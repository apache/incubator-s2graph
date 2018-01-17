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
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.{BaseDirectory, RAMDirectory}
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.mysqls.GlobalIndex
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{EdgeId, S2EdgeLike, S2VertexLike}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.Json

import scala.concurrent.Future


class LuceneIndexProvider(config: Config) extends IndexProvider {
  import GlobalIndex._
  import IndexProvider._

  import scala.collection.JavaConverters._
  import scala.collection.mutable

  val analyzer = new StandardAnalyzer()
  val writers = mutable.Map.empty[String, IndexWriter]
  val directories = mutable.Map.empty[String, BaseDirectory]

  private def getOrElseCreateIndexWriter(indexName: String): IndexWriter = {
    writers.getOrElseUpdate(indexName, {
      val dir = directories.getOrElseUpdate(indexName, new RAMDirectory())
      val indexConfig = new IndexWriterConfig(analyzer)
      new IndexWriter(dir, indexConfig)
    })
  }

  private def toDocument(vertex: S2VertexLike, forceToIndex: Boolean): Option[Document] = {
    val props = vertex.props.asScala
    val storeInGlobalIndex = if (forceToIndex) true else props.exists(_._2.columnMeta.storeInGlobalIndex)

    if (!storeInGlobalIndex) None
    else {
      val doc = new Document()
      val id = vertex.id.toString

      doc.add(new StringField(vidField, id, Field.Store.YES))
      doc.add(new StringField(serviceField, vertex.serviceName, Field.Store.YES))
      doc.add(new StringField(serviceColumnField, vertex.columnName, Field.Store.YES))

      props.foreach { case (dim, s2VertexProperty) =>
        val columnMeta = s2VertexProperty.columnMeta
        if (columnMeta.seq > 0) {
          val shouldIndex = if (columnMeta.storeInGlobalIndex) Field.Store.YES else Field.Store.NO

          val field = columnMeta.dataType match {
            case "string" => new StringField(dim, s2VertexProperty.innerVal.value.toString, shouldIndex)
            case _ => new StringField(dim, s2VertexProperty.innerVal.value.toString, shouldIndex)
          }
          doc.add(field)
        }
      }

      Option(doc)
    }
  }

  private def toDocument(edge: S2EdgeLike, forceToIndex: Boolean): Option[Document] = {
    val props = edge.getPropsWithTs().asScala
    val store = if (forceToIndex) true else props.exists(_._2.labelMeta.storeInGlobalIndex)

    if (!store) None
    else {
      val doc = new Document()
      val id = edge.edgeId.toString

      doc.add(new StringField(eidField, id, Field.Store.YES))
      doc.add(new StringField(serviceField, edge.serviceName, Field.Store.YES))
      doc.add(new StringField(labelField, edge.label(), Field.Store.YES))

      props.foreach { case (dim, s2Property) =>
        val shouldIndex = if (s2Property.labelMeta.storeInGlobalIndex) Field.Store.YES else Field.Store.NO

        val field = s2Property.labelMeta.dataType match {
          case "string" => new StringField(dim, s2Property.innerVal.value.toString, shouldIndex)
          case _ => new StringField(dim, s2Property.innerVal.value.toString, shouldIndex)
        }
        doc.add(field)
      }

      Option(doc)
    }
  }

  override def mutateEdgesAsync(edges: Seq[S2EdgeLike], forceToIndex: Boolean = false): Future[Seq[Boolean]] =
    Future.successful(mutateEdges(edges, forceToIndex))

  override def mutateVertices(vertices: Seq[S2VertexLike], forceToIndex: Boolean = false): Seq[Boolean] = {
    val writer = getOrElseCreateIndexWriter(GlobalIndex.VertexIndexName)

    vertices.foreach { vertex =>
      toDocument(vertex, forceToIndex).foreach { doc =>
        val vId = vertex.id.toString()

        writer.updateDocument(new Term(IdField, vId), doc)
      }
    }

    writer.commit()

    vertices.map(_ => true)
  }

  override def mutateVerticesAsync(vertices: Seq[S2VertexLike], forceToIndex: Boolean = false): Future[Seq[Boolean]] =
    Future.successful(mutateVertices(vertices, forceToIndex))

  override def mutateEdges(edges: Seq[S2EdgeLike], forceToIndex: Boolean = false): Seq[Boolean] = {
    val writer = getOrElseCreateIndexWriter(GlobalIndex.EdgeIndexName)

    edges.foreach { edge =>
      toDocument(edge, forceToIndex).foreach { doc =>
        val eId = edge.edgeId.toString

        writer.updateDocument(new Term(IdField, eId), doc)
      }
    }

    writer.commit()

    edges.map(_ => true)
  }

  override def fetchEdgeIds(hasContainers: java.util.List[HasContainer]): java.util.List[EdgeId] = {
    val field = eidField
    val ids = new java.util.HashSet[EdgeId]

    val queryString = buildQueryString(hasContainers)

    try {
      val q = new QueryParser(field, analyzer).parse(queryString)

      val reader = DirectoryReader.open(directories(GlobalIndex.EdgeIndexName))
      val searcher = new IndexSearcher(reader)

      val docs = searcher.search(q, hitsPerPage)

      docs.scoreDocs.foreach { scoreDoc =>
        val document = searcher.doc(scoreDoc.doc)
        val id = Conversions.s2EdgeIdReads.reads(Json.parse(document.get(field))).get
        ids.add(id);
      }

      reader.close()
      ids
    } catch {
      case ex: ParseException =>
        logger.error(s"[IndexProvider]: ${queryString} parse failed.", ex)
        ids
    }

    new util.ArrayList[EdgeId](ids)
  }

  override def fetchVertexIds(hasContainers: java.util.List[HasContainer]): java.util.List[VertexId] = {
    val field = vidField
    val ids = new java.util.HashSet[VertexId]
    val queryString = buildQueryString(hasContainers)

    try {
      val q = new QueryParser(field, analyzer).parse(queryString)

      val reader = DirectoryReader.open(directories(GlobalIndex.VertexIndexName))
      val searcher = new IndexSearcher(reader)

      val docs = searcher.search(q, hitsPerPage)

      docs.scoreDocs.foreach { scoreDoc =>
        val document = searcher.doc(scoreDoc.doc)
        val id = Conversions.s2VertexIdReads.reads(Json.parse(document.get(field))).get
        ids.add(id)
      }

      reader.close()
      ids
    } catch {
      case ex: ParseException =>
        logger.error(s"[IndexProvider]: ${queryString} parse failed.", ex)
        ids
    }

    new util.ArrayList[VertexId](ids)
  }

  override def shutdown(): Unit = {
    writers.foreach { case (_, writer) => writer.close() }
  }

  override def fetchEdgeIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[EdgeId]] = Future.successful(fetchEdgeIds(hasContainers))

  override def fetchVertexIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[VertexId]] = Future.successful(fetchVertexIds(hasContainers))
}

