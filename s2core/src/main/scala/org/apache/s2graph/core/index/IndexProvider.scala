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
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.{BaseDirectory, RAMDirectory}
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.{EdgeId, S2Edge, S2Vertex}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.{InnerValLike, VertexId}
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.process.traversal.{Compare, Contains, P}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.process.traversal.util.{AndP, OrP}
import play.api.libs.json.Json

import scala.concurrent.Future

object IndexProvider {
  val vidField = "_vid_"
  val eidField = "_eid_"
  val labelField = "_label_"
  val serviceField = "_service_"
  val serviceColumnField = "_serviceColumn_"

  def apply(config: Config): IndexProvider = {
    val indexProviderType = "lucene"
//      if (config.hasPath("index.provider")) config.getString("index.provider") else "lucene"

    indexProviderType match {
      case "lucene" => new LuceneIndexProvider(config)
    }
  }

  def buildQuerySingleString(container: HasContainer): String = {
    import scala.collection.JavaConversions._

    val key = container.getKey
    val value = container.getValue

    val biPredicate = container.getBiPredicate

    biPredicate match {
      case Contains.within =>
        key + ":(" + value.asInstanceOf[util.Collection[_]].toSeq.mkString(" OR ") + ")"
      case Contains.without =>
        key + ":NOT (" + value.asInstanceOf[util.Collection[_]].toSeq.mkString(" AND ") + ")"
      case Compare.eq => s"${key}:${value}"
      case Compare.gte => s"${key}:[${value} TO *] AND NOT ${key}:${value}"
      case Compare.gt => s"${key}:[${value} TO *]"
      case Compare.lte => s"${key}:[* TO ${value}]"
      case Compare.lt => s"${key}:[* TO ${value}] AND NOT ${key}:${value}"
      case Compare.neq => s"NOT ${key}:${value}"
      case _ => throw new IllegalArgumentException("not supported yet.")
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

  def mutateVertices(vertices: Seq[S2Vertex]): Seq[Boolean]
  def mutateVerticesAsync(vertices: Seq[S2Vertex]): Future[Seq[Boolean]]

  def mutateEdges(edges: Seq[S2Edge]): Seq[Boolean]
  def mutateEdgesAsync(edges: Seq[S2Edge]): Future[Seq[Boolean]]

  def shutdown(): Unit
}

class LuceneIndexProvider(config: Config) extends IndexProvider {
  import IndexProvider._
  import scala.collection.mutable
  import scala.collection.JavaConverters._

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

  private def toDocument(globalIndex: GlobalIndex, vertex: S2Vertex): Option[Document] = {
    val props = vertex.props.asScala
    val exist = props.exists(t => globalIndex.propNamesSet(t._1))
    if (!exist) None
    else {
      val doc = new Document()
      val id = vertex.id.toString

      doc.add(new StringField(vidField, id, Field.Store.YES))
      doc.add(new StringField(serviceField, vertex.serviceName, Field.Store.YES))
      doc.add(new StringField(serviceColumnField, vertex.columnName, Field.Store.YES))

      props.foreach { case (dim, s2VertexProperty) =>
        val shouldIndex = if (globalIndex.propNamesSet(dim)) Field.Store.YES else Field.Store.NO
        val field = s2VertexProperty.columnMeta.dataType match {
          case "string" => new StringField(dim, s2VertexProperty.innerVal.value.toString, shouldIndex)
          case _ => new StringField(dim, s2VertexProperty.innerVal.value.toString, shouldIndex)
        }
        doc.add(field)
      }

      Option(doc)
    }
  }

  private def toDocument(globalIndex: GlobalIndex, edge: S2Edge): Option[Document] = {
    val props = edge.propsWithTs.asScala
    val exist = props.exists(t => globalIndex.propNamesSet(t._1))
    if (!exist) None
    else {
      val doc = new Document()
      val id = edge.edgeId.toString

      doc.add(new StringField(eidField, id, Field.Store.YES))
      doc.add(new StringField(serviceField, edge.serviceName, Field.Store.YES))
      doc.add(new StringField(labelField, edge.label(), Field.Store.YES))

      props.foreach { case (dim, s2Property) =>
        val shouldIndex = if (globalIndex.propNamesSet(dim)) Field.Store.YES else Field.Store.NO
        val field = s2Property.labelMeta.dataType match {
          case "string" => new StringField(dim, s2Property.innerVal.value.toString, shouldIndex)
          case _ => new StringField(dim, s2Property.innerVal.value.toString, shouldIndex)
        }
        doc.add(field)
      }

      Option(doc)
    }
  }

  override def mutateVertices(vertices: Seq[S2Vertex]): Seq[Boolean] = {
    val globalIndexOptions = GlobalIndex.findAll()

    globalIndexOptions.map { globalIndex =>
      val writer = getOrElseCreateIndexWriter(globalIndex.indexName)

      vertices.foreach { vertex =>
        toDocument(globalIndex, vertex).foreach { doc =>
          writer.addDocument(doc)
        }
      }

      writer.commit()
    }

    vertices.map(_ => true)
  }

  override def mutateEdges(edges: Seq[S2Edge]): Seq[Boolean] = {
    val globalIndexOptions = GlobalIndex.findAll()

    globalIndexOptions.map { globalIndex =>
      val writer = getOrElseCreateIndexWriter(globalIndex.indexName)

      edges.foreach { edge =>
        toDocument(globalIndex, edge).foreach { doc =>
          writer.addDocument(doc)
        }
      }

      writer.commit()
    }

    edges.map(_ => true)
  }

  override def fetchEdgeIds(hasContainers: java.util.List[HasContainer]): java.util.List[EdgeId] = {
    val field = eidField
    val ids = new java.util.ArrayList[EdgeId]

    GlobalIndex.findGlobalIndex(hasContainers).map { globalIndex =>
      val queryString = buildQueryString(hasContainers)

      try {
        val q = new QueryParser(field, analyzer).parse(queryString)
        val hitsPerPage = 10
        val reader = DirectoryReader.open(directories(globalIndex.indexName))
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
    }

    ids
  }

  override def fetchVertexIds(hasContainers: java.util.List[HasContainer]): java.util.List[VertexId] = {
    val field = vidField
    val ids = new java.util.ArrayList[VertexId]
    GlobalIndex.findGlobalIndex(hasContainers).map { globalIndex =>
      val queryString = buildQueryString(hasContainers)

      try {
        val q = new QueryParser(field, analyzer).parse(queryString)
        val hitsPerPage = 10
        val reader = DirectoryReader.open(directories(globalIndex.indexName))
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
    }

    ids
  }

  override def shutdown(): Unit = {
    writers.foreach { case (_, writer) => writer.close() }
  }

  override def fetchEdgeIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[EdgeId]] = Future.successful(fetchEdgeIds(hasContainers))

  override def fetchVertexIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[VertexId]] = Future.successful(fetchVertexIds(hasContainers))

  override def mutateVerticesAsync(vertices: Seq[S2Vertex]): Future[Seq[Boolean]] = Future.successful(mutateVertices(vertices))

  override def mutateEdgesAsync(edges: Seq[S2Edge]): Future[Seq[Boolean]] = Future.successful(mutateEdges(edges))
}
