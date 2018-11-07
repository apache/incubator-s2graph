package org.apache.s2graph.core.storage.datastore

import com.spotify.asyncdatastoreclient.{Query => _, _}
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.MutateResponse

import scala.concurrent.{ExecutionContext, Future}

class DatastoreEdgeMutator(graph: S2GraphLike,
                           datastore: Datastore) extends EdgeMutator {

  import DatastoreStorage._

  //TODO: pool of datastore?(lookup by zkQuorum)
  override def mutateStrongEdges(zkQuorum: String,
                                 _edges: Seq[S2EdgeLike],
                                 withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = ???

  override def mutateWeakEdges(zkQuorum: String,
                               _edges: Seq[S2EdgeLike],
                               withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] = {
    val batch = QueryBuilder.batch()

    _edges.foreach { edge =>
      batch.add(toMutationStatement(edge))
    }

    //TODO: need to ensure the index of parameter sequence with correct return type
    asScala(datastore.executeAsync(batch)).map { _ =>
      (0 until _edges.size).map(_ -> true)
    }
  }

  override def incrementCounts(zkQuorum: String,
                               edges: Seq[S2EdgeLike],
                               withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[MutateResponse]] = ???

  override def updateDegree(zkQuorum: String,
                            edge: S2EdgeLike,
                            degreeVal: Long)(implicit ec: ExecutionContext): Future[MutateResponse] = ???

  override def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult,
                                             requestTs: Long,
                                             retryNum: Int)(implicit ec: ExecutionContext): Future[Boolean] = ???
}
