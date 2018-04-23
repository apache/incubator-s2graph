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

import java.io.File
import java.util

import com.typesafe.config.Config
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.{BaseDirectory, RAMDirectory, SimpleFSDirectory}
import org.apache.lucene.search.TopScoreDocCollector
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.mysqls.GlobalIndex
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{EdgeId, S2EdgeLike, S2VertexLike, VertexQueryParam}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.{Json, Reads}

import scala.concurrent.Future


class LuceneIndexProvider(config: Config) extends IndexProvider {

  import GlobalIndex._
  import IndexProvider._

  import scala.collection.JavaConverters._
  import scala.collection.mutable

  val analyzer = new KeywordAnalyzer()
  val writers = mutable.Map.empty[String, IndexWriter]
  val directories = mutable.Map.empty[String, BaseDirectory]
  val baseDirectory = scala.util.Try(config.getString("index.provider.base.dir")).getOrElse(".")
  val MAX_RESULTS = 100000

  private def getOrElseDirectory(indexName: String): BaseDirectory = {
    val fsType = scala.util.Try(config.getString("index.provider.lucene.fsType")).getOrElse("memory")

    val fsDir = if (fsType == "memory") {
      new RAMDirectory()
    } else {
      val pathname = s"${baseDirectory}/${indexName}"
      new SimpleFSDirectory(new File(pathname).toPath)
    }

    val dir = directories.getOrElseUpdate(indexName, fsDir)

    dir
  }

  private def getOrElseCreateIndexWriter(indexName: String): IndexWriter = {
    writers.getOrElseUpdate(indexName, {
      val dir = getOrElseDirectory(indexName)

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
        //        logger.error(s"DOC: ${doc}")

        writer.updateDocument(new Term(vidField, vId), doc)
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

        writer.updateDocument(new Term(eidField, eId), doc)
      }
    }

    writer.commit()

    edges.map(_ => true)
  }

  private def fetchInner[T](q: Query, offset: Int, limit: Int, indexKey: String, field: String, reads: Reads[T]): util.List[T] = {
    val ids = new java.util.HashSet[T]

    var reader: DirectoryReader = null
    try {
      val reader = DirectoryReader.open(getOrElseDirectory(indexKey))
      val searcher = new IndexSearcher(reader)
      val collector = TopScoreDocCollector.create(MAX_RESULTS)
      val startIndex = offset

      searcher.search(q, collector)

      val hits = collector.topDocs(startIndex, limit)
      hits.scoreDocs.foreach { scoreDoc =>
        val document = searcher.doc(scoreDoc.doc)
        val id = reads.reads(Json.parse(document.get(field))).get
        ids.add(id)
      }
    } catch {
      case e: org.apache.lucene.index.IndexNotFoundException => logger.info("Index file not found.")
    } finally {
      if (reader != null) reader.close()
    }

    new util.ArrayList[T](ids)
  }

  override def fetchVertexIds(hasContainers: java.util.List[HasContainer]): java.util.List[VertexId] = {
    val field = vidField
    val queryString = buildQueryString(hasContainers)

    try {
      val q = new QueryParser(field, analyzer).parse(queryString)
      fetchInner[VertexId](q, 0, 100, GlobalIndex.VertexIndexName, vidField, Conversions.s2VertexIdReads)
    } catch {
      case ex: ParseException =>
        logger.error(s"[IndexProvider]: ${queryString} parse failed.", ex)
        util.Arrays.asList[VertexId]()
    }
  }

  override def fetchEdgeIds(hasContainers: java.util.List[HasContainer]): java.util.List[EdgeId] = {
    val field = eidField
    val queryString = buildQueryString(hasContainers)

    try {
      val q = new QueryParser(field, analyzer).parse(queryString)
      fetchInner[EdgeId](q, 0, 100, GlobalIndex.EdgeIndexName, field, Conversions.s2EdgeIdReads)
    } catch {
      case ex: ParseException =>
        logger.error(s"[IndexProvider]: ${queryString} parse failed.", ex)
        util.Arrays.asList[EdgeId]()
    }
  }

  override def fetchEdgeIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[EdgeId]] = Future.successful(fetchEdgeIds(hasContainers))

  override def fetchVertexIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[VertexId]] = Future.successful(fetchVertexIds(hasContainers))

  override def fetchVertexIdsAsyncRaw(vertexQueryParam: VertexQueryParam): Future[util.List[VertexId]] = {
    val ret = vertexQueryParam.searchString.fold(util.Arrays.asList[VertexId]()) { queryString =>
      val field = vidField
      try {
        val q = new QueryParser(field, analyzer).parse(queryString)
        fetchInner[VertexId](q, vertexQueryParam.offset, vertexQueryParam.limit, GlobalIndex.VertexIndexName, vidField, Conversions.s2VertexIdReads)
      } catch {
        case ex: ParseException =>
          logger.error(s"[IndexProvider]: ${queryString} parse failed.", ex)
          util.Arrays.asList[VertexId]()
      }
    }

    Future.successful(ret)
  }

  override def shutdown(): Unit = {
    writers.foreach { case (_, writer) => writer.close() }
  }

}

