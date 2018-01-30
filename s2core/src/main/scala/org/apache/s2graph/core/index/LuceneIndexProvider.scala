package org.apache.s2graph.core.index

import java.util

import com.typesafe.config.Config
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.{BaseDirectory, RAMDirectory}
import org.apache.s2graph.core.{EdgeId, S2EdgeLike, S2VertexLike}
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.mysqls.GlobalIndex
import org.apache.s2graph.core.mysqls.GlobalIndex.{eidField, labelField, serviceColumnField, serviceField, vidField}
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.Json

import scala.concurrent.Future


class LuceneIndexProvider(config: Config) extends IndexProvider {
  import IndexProvider._
  import scala.collection.mutable
  import scala.collection.JavaConverters._
  import GlobalIndex._

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

  private def toDocument(globalIndex: GlobalIndex, vertex: S2VertexLike): Option[Document] = {
    val props = vertex.props.asScala
    val filtered = props.filterKeys(globalIndex.propNamesSet)

    if (filtered.isEmpty) None
    else {
      val doc = new Document()
      val id = vertex.id.toString

      doc.add(new StringField(vidField, id, Field.Store.YES))
      doc.add(new StringField(serviceField, vertex.serviceName, Field.Store.YES))
      doc.add(new StringField(serviceColumnField, vertex.columnName, Field.Store.YES))

      filtered.foreach { case (dim, s2VertexProperty) =>
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

  private def toDocument(globalIndex: GlobalIndex, edge: S2EdgeLike): Option[Document] = {
    val props = edge.getPropsWithTs().asScala
    val filtered = props.filterKeys(globalIndex.propNamesSet)

    if (filtered.isEmpty) None
    else {
      val doc = new Document()
      val id = edge.edgeId.toString

      doc.add(new StringField(eidField, id, Field.Store.YES))
      doc.add(new StringField(serviceField, edge.serviceName, Field.Store.YES))
      doc.add(new StringField(labelField, edge.label(), Field.Store.YES))

      filtered.foreach { case (dim, s2Property) =>
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

  override def mutateVertices(vertices: Seq[S2VertexLike]): Seq[Boolean] = {
    val globalIndexOptions = GlobalIndex.findAll(GlobalIndex.VertexType)

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

  override def mutateEdges(edges: Seq[S2EdgeLike]): Seq[Boolean] = {
    val globalIndexOptions = GlobalIndex.findAll(GlobalIndex.EdgeType)

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
    val ids = new java.util.HashSet[EdgeId]

    GlobalIndex.findGlobalIndex(GlobalIndex.EdgeType, hasContainers).map { globalIndex =>
      val queryString = buildQueryString(hasContainers)

      try {
        val q = new QueryParser(field, analyzer).parse(queryString)

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

    new util.ArrayList[EdgeId](ids)
  }

  override def fetchVertexIds(hasContainers: java.util.List[HasContainer]): java.util.List[VertexId] = {
    val field = vidField
    val ids = new java.util.HashSet[VertexId]

    GlobalIndex.findGlobalIndex(GlobalIndex.VertexType, hasContainers).map { globalIndex =>
      val queryString = buildQueryString(hasContainers)

      try {
        val q = new QueryParser(field, analyzer).parse(queryString)

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

    new util.ArrayList[VertexId](ids)
  }

  override def shutdown(): Unit = {
    writers.foreach { case (_, writer) => writer.close() }
  }

  override def fetchEdgeIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[EdgeId]] = Future.successful(fetchEdgeIds(hasContainers))

  override def fetchVertexIdsAsync(hasContainers: java.util.List[HasContainer]): Future[util.List[VertexId]] = Future.successful(fetchVertexIds(hasContainers))

  override def mutateVerticesAsync(vertices: Seq[S2VertexLike]): Future[Seq[Boolean]] = Future.successful(mutateVertices(vertices))

  override def mutateEdgesAsync(edges: Seq[S2EdgeLike]): Future[Seq[Boolean]] = Future.successful(mutateEdges(edges))
}

