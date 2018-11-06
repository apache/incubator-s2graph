package org.apache.s2graph.core.storage.datastore


import com.google.common.util.concurrent._
import org.apache.s2graph.core.{S2GraphLike, S2VertexLike, VertexFetcher, VertexQueryParam}
import com.spotify.asyncdatastoreclient._
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.schema.ServiceColumn

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

object DatastoreVertexFetcher {
  def asScala[V](lf: ListenableFuture[V]): Future[V] = {
    val p = Promise[V]

    Futures.addCallback(lf, new FutureCallback[V] {
      def onFailure(t: Throwable): Unit = p failure t
      def onSuccess(result: V): Unit    = p success result
    })

    p.future
  }
}
class DatastoreVertexFetcher(graph: S2GraphLike,
                             dsService: Datastore) extends VertexFetcher {
  import DatastoreVertexMutator._
  import DatastoreVertexFetcher._

  override def fetchVertices(vertexQueryParam: VertexQueryParam)(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = {
    val keys = vertexQueryParam.vertexIds.map { vertexId =>
      QueryBuilder.query(vertexId.column.service.hTableName, vertexId.toString())
    }.asJava

    asScala(dsService.executeAsync(keys)).map { queryResult =>
      queryResult.getAll().asScala
        .map(fromEntity(graph, _))
        .filter(vertexQueryParam.where.getOrElse(WhereParser.success).filter(_))
    }
  }

  override def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = {
    val query = QueryBuilder.query()
    asScala(dsService.executeAsync(query)).map { queryResult =>
      queryResult.getAll().asScala.map { entity =>
        fromEntity(graph, entity)
      }
    }

    val futures = ServiceColumn.findAll().groupBy(_.service.hTableName).toSeq.map { case (hTableName, columns) =>
      val distinctColumns = columns.toSet
      val query = QueryBuilder.query().kindOf(hTableName)
      asScala(dsService.executeAsync(query)).map { queryResult =>
        queryResult.getAll().asScala.map { entity =>
          fromEntity(graph, entity)
        }.filter(v => distinctColumns(v.serviceColumn))
      }
    }

    Future.sequence(futures).map(_.flatten)
  }
}
