package org.apache.s2graph.core.storage.datastore


import com.spotify.asyncdatastoreclient._
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.schema.ServiceColumn
import org.apache.s2graph.core.{S2GraphLike, S2VertexLike, VertexFetcher, VertexQueryParam}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class DatastoreVertexFetcher(graph: S2GraphLike,
                             datastore: Datastore) extends VertexFetcher {

  import DatastoreStorage._

  override def fetchVertices(vertexQueryParam: VertexQueryParam)(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = {
    val keys = vertexQueryParam.vertexIds.map { vertexId =>
      QueryBuilder.query(vertexId.column.service.hTableName, vertexId.toString())
    }.asJava

    asScala(datastore.executeAsync(keys)).map { queryResult =>
      queryResult.getAll().asScala
        .map(toS2Vertex(graph, _))
        .filter(vertexQueryParam.where.getOrElse(WhereParser.success).filter(_))
    }
  }

  override def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = {
    val query = QueryBuilder.query()
    asScala(datastore.executeAsync(query)).map { queryResult =>
      queryResult.getAll().asScala.map { entity =>
        toS2Vertex(graph, entity)
      }
    }

    val futures = ServiceColumn.findAll().groupBy(_.service.hTableName).toSeq.map { case (hTableName, columns) =>
      val distinctColumns = columns.toSet
      val query = QueryBuilder.query().kindOf(hTableName)
      asScala(datastore.executeAsync(query)).map { queryResult =>
        queryResult.getAll().asScala.map { entity =>
          toS2Vertex(graph, entity)
        }.filter(v => distinctColumns(v.serviceColumn))
      }
    }

    Future.sequence(futures).map(_.flatten)
  }
}
