package org.apache.s2graph.core.index

import com.typesafe.config.Config
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.RAMDirectory
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.{EdgeId, S2Edge, S2Vertex}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.{InnerValLike, VertexId}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.Json

object IndexProvider {
  val vidField = "_vid_"
  val eidField = "_eid_"

  def apply(config: Config): IndexProvider = {
    val indexProviderType = "lucene"
//      if (config.hasPath("index.provider")) config.getString("index.provider") else "lucene"

    indexProviderType match {
      case "lucene" => new LuceneIndexProvider(config)
    }
  }

  def buildQueryString(hasContainers: java.util.List[HasContainer]): String = {
    import scala.collection.JavaConversions._
    hasContainers.map { container =>
      container.getKey + ":" + container.getValue
    }.mkString(" AND ")
  }

}

trait IndexProvider {
  //TODO: Seq nee do be changed into stream
  def fetchEdgeIds(queryString: String): java.util.List[EdgeId]

  def fetchVertexIds(queryString: String): java.util.List[VertexId]

  def mutateVertices(vertices: Seq[S2Vertex]): Seq[Boolean]

  def mutateEdges(edges: Seq[S2Edge]): Seq[Boolean]

  def shutdown(): Unit
}

class LuceneIndexProvider(config: Config) extends IndexProvider {
  import IndexProvider._

  val analyzer = new StandardAnalyzer()
  val directory = new RAMDirectory()
  val indexConfig = new IndexWriterConfig(analyzer)
  val writer = new IndexWriter(directory, indexConfig)

  override def mutateVertices(vertices: Seq[S2Vertex]): Seq[Boolean] = {
    vertices.map { vertex =>
      val doc = new Document()
      val id = vertex.id.toString()
      doc.add(new StringField(vidField, id, Field.Store.YES))

      vertex.properties.foreach { case (dim, value) =>
        doc.add(new TextField(dim, value.toString, Field.Store.YES))
      }
      writer.addDocument(doc)
    }
    writer.commit()
    vertices.map(_ => true)
  }

  override def mutateEdges(edges: Seq[S2Edge]): Seq[Boolean] = {
    edges.map { edge =>
      val doc = new Document()
      val id = edge.edgeId.toString
      doc.add(new StringField(eidField, id, Field.Store.YES))

      edge.properties.foreach { case (dim, value) =>
        doc.add(new TextField(dim, value.toString, Field.Store.YES))
      }
      writer.addDocument(doc)
    }
    writer.commit()
    edges.map(_ => true)
  }

  override def fetchEdgeIds(queryString: String): java.util.List[EdgeId] = {
    val field = eidField
    val ids = new java.util.ArrayList[EdgeId]
    val q = new QueryParser(field, analyzer).parse(queryString)
    val hitsPerPage = 10
    val reader = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)

    val docs = searcher.search(q, hitsPerPage)

    docs.scoreDocs.foreach { scoreDoc =>
      val document = searcher.doc(scoreDoc.doc)
      val id = Conversions.s2EdgeIdReads.reads(Json.parse(document.get(field))).get
      ids.add(id);
    }

    reader.close()
    ids
  }

  override def fetchVertexIds(queryString: String): java.util.List[VertexId] = {
    val field = vidField
    val ids = new java.util.ArrayList[VertexId]
    val q = new QueryParser(field, analyzer).parse(queryString)
    val hitsPerPage = 10
    val reader = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)

    val docs = searcher.search(q, hitsPerPage)

    docs.scoreDocs.foreach { scoreDoc =>
      val document = searcher.doc(scoreDoc.doc)
      val id = Conversions.s2VertexIdReads.reads(Json.parse(document.get(field))).get
      ids.add(id)
    }

    reader.close()
    ids
  }
  override def shutdown(): Unit = {
    writer.close()
  }

}