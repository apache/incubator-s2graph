package org.apache.s2graph.core.index

import com.typesafe.config.Config
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.RAMDirectory
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.{EdgeId, S2Edge}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.InnerValLike
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.Json

object IndexProvider {
  val edgeIdField = "_edgeId_"
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
  def fetchIds(queryString: String): java.util.List[String]

  def mutateEdges(edges: Seq[S2Edge]): Seq[Boolean]

  def shutdown(): Unit
}

class LuceneIndexProvider(config: Config) extends IndexProvider {
  import IndexProvider._

  val analyzer = new StandardAnalyzer()
  val directory = new RAMDirectory()
  val indexConfig = new IndexWriterConfig(analyzer)
  val writer = new IndexWriter(directory, indexConfig)

  override def mutateEdges(edges: Seq[S2Edge]): Seq[Boolean] = {
    edges.map { edge =>
      val doc = new Document()
      val edgeIdString = edge.edgeId.toString
      doc.add(new StringField(edgeIdField, edgeIdString, Field.Store.YES))

      edge.properties.foreach { case (dim, value) =>
        doc.add(new TextField(dim, value.toString, Field.Store.YES))
      }
      writer.addDocument(doc)
    }
    writer.commit()
    edges.map(_ => true)
  }

  override def fetchIds(queryString: String): java.util.List[String] = {
    val ids = new java.util.ArrayList[String]
    val q = new QueryParser(edgeIdField, analyzer).parse(queryString)
    val hitsPerPage = 10
    val reader = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)

    val docs = searcher.search(q, hitsPerPage)

    docs.scoreDocs.foreach { scoreDoc =>
      val document = searcher.doc(scoreDoc.doc)
      ids.add(document.get(edgeIdField))
    }

    reader.close()
    ids
  }

  override def shutdown(): Unit = {
    writer.close()
  }
}