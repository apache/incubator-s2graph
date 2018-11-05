package org.apache.s2graph.core.storage.datastore

import org.apache.s2graph.core.{EdgeMutator, S2EdgeLike, StepResult}
import org.apache.s2graph.core.storage.MutateResponse

import scala.concurrent.{ExecutionContext, Future}

class DatastoreEdgeMutator extends EdgeMutator {
  override def mutateStrongEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = ???

  override def mutateWeakEdges(zkQuorum: String, _edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[(Int, Boolean)]] = ???

  override def incrementCounts(zkQuorum: String, edges: Seq[S2EdgeLike], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[MutateResponse]] = ???

  override def updateDegree(zkQuorum: String, edge: S2EdgeLike, degreeVal: Long)(implicit ec: ExecutionContext): Future[MutateResponse] = ???

  override def deleteAllFetchedEdgesAsyncOld(stepInnerResult: StepResult, requestTs: Long, retryNum: Int)(implicit ec: ExecutionContext): Future[Boolean] = ???
}
