package org.apache.s2graph.core.storage.datastore

import com.spotify.asyncdatastoreclient.Datastore
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

class DatastoreEdgeFetcher(graph: S2GraphLike,
                           datastore: Datastore) extends EdgeFetcher {

  import DatastoreStorage._

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val futures = queryRequests.map { queryRequest =>
      val queryParam = queryRequest.queryParam
      val query = toQuery(queryRequest)

      asScala(datastore.executeAsync(query)).map { queryResult =>
        val edges = queryResult.getAll.asScala
          .map(toS2Edge(graph, _))

        val edgeWithScores = edges.map(EdgeWithScore(_, 1.0, queryParam.label))
        StepResult(edgeWithScores = edgeWithScores, Nil, Nil)
      }
    }

    Future.sequence(futures)
  }

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = {
    //    val futures = Label.findAll().groupBy(_.hbaseTableName).toSeq.map { case (hTableName, labels) =>
    //      val distinctLabels = labels.toSet
    //      asScala(datastore.executeAsync(QueryBuilder.query().kindOf(toKind(hTableName)))).map { queryResult =>
    //        queryResult.getAll().asScala.map { entity =>
    //          edgeFromEntity(graph, entity)
    //        }.filter(e => distinctLabels(e.innerLabel))
    //      }
    //    }
    //
    //    Future.sequence(futures).map(_.flatten)
    ???
  }
}
