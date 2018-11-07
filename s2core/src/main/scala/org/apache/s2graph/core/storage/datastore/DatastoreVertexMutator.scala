package org.apache.s2graph.core.storage.datastore


import com.spotify.asyncdatastoreclient._
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.MutateResponse

import scala.concurrent.{ExecutionContext, Future}

class DatastoreVertexMutator(graph: S2GraphLike,
                             datastore: Datastore) extends VertexMutator {

  import DatastoreStorage._
  // pool of datastores and lookup by zkQuorum?

  override def mutateVertex(zkQuorum: String,
                            vertex: S2VertexLike,
                            withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] = {

    asScala(datastore.executeAsync(toMutationStatement(vertex))).map { _ =>
      MutateResponse.Success
    }
  }
}
