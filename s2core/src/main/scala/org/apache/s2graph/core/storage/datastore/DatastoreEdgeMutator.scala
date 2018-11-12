package org.apache.s2graph.core.storage.datastore

import com.google.common.util.concurrent.ListenableFuture
import com.spotify.asyncdatastoreclient.{Datastore, QueryBuilder, TransactionResult}
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.MutateResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

class DatastoreEdgeMutator(graph: S2GraphLike,
                           datastore: Datastore) extends EdgeMutator {

  import DatastoreStorage._

  def mutateSnapshotEdge(snapshotEdge: SnapshotEdge)(implicit ec: ExecutionContext): Future[MutateResponse] = {
    asScala(datastore.executeAsync(toMutationStatement(snapshotEdge))).map { _ =>
      MutateResponse.Success
    }
  }

  def fetchSnapshotEdge(snapshotEdge: SnapshotEdge)(implicit ec: ExecutionContext): Future[Option[S2EdgeLike]] = {
    asScala(datastore.executeAsync(toQuery(snapshotEdge))).map { queryResult =>
      queryResult.getAll.asScala.headOption.map(toSnapshotEdge(graph, _).edge)
    }
  }

  def fetchAndDeletes(edges: Seq[S2EdgeLike])(implicit ec: ExecutionContext) = {
    if (edges.isEmpty) Future.successful(MutateResponse.Success)
    else {
      asScala(datastore.executeAsync(toQuery(edges.head))).flatMap { queryResult =>
        val batch = QueryBuilder.batch()
        queryResult.getAll.asScala.map { entity =>
          batch.add(QueryBuilder.delete(entity.getKey()))
        }
        asScala(datastore.executeAsync(batch)).map { _ => MutateResponse.Success}
      }
    }
  }

  //TODO: pool of datastore?(lookup by zkQuorum)
  override def mutateStrongEdges(zkQuorum: String,
                                 _edges: Seq[S2EdgeLike],
                                 withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    val grouped = _edges.groupBy { edge =>
      edge.toSnapshotEdge.edge.edgeId
    }

    val futures = grouped.map { case (_, edges) =>
      val (squashedEdge, _) = S2Edge.buildOperation(None, edges)
      // first delete all indexed edges.
      val (outEdges, inEdges) = edges.partition(_.getDirection() == "out")
      
      fetchAndDeletes(outEdges).flatMap { _ =>
        fetchAndDeletes(inEdges).flatMap { _ =>
//          val mutations = toMutationStatement(squashedEdge)
          val mutations = toBatch(squashedEdge)
          asScala(datastore.executeAsync(mutations))
        }
      }
    }

    //TODO: need to ensure the index of parameter sequence with correct return type
    Future.sequence(futures).map(_.map(_ => true).toSeq)
  }

  override def mutateWeakEdges(zkQuorum: String,
                               _edges: Seq[S2EdgeLike],
                               withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] = {
    val batch = QueryBuilder.batch()

    _edges.foreach { edge =>
      toBatch(edge, batch)
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
