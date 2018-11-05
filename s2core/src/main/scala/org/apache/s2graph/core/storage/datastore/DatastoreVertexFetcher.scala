package org.apache.s2graph.core.storage.datastore

import com.google.appengine.api.datastore.DatastoreService
import org.apache.s2graph.core.{S2GraphLike, S2VertexLike, VertexFetcher, VertexQueryParam}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class DatastoreVertexFetcher(graph: S2GraphLike,
                             dsService: DatastoreService) extends VertexFetcher {
  import DatastoreVertexMutator._

  override def fetchVertices(vertexQueryParam: VertexQueryParam)(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = {
    val keys = toKeys(vertexQueryParam.vertexIds).asJava

    val vertices = dsService.get(keys).asScala.map { case (_, entity) =>
      fromEntity(graph, entity)
    }.toSeq

    Future.successful(vertices)
  }

  override def fetchVerticesAll()(implicit ec: ExecutionContext): Future[Seq[S2VertexLike]] = ???
}
