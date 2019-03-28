package org.apache.s2graph.core.storage.datastore

import com.spotify.asyncdatastoreclient.{Datastore, QueryBuilder}
import org.apache.s2graph.core._
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.storage.StorageIO
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.utils.{DeferCache, logger}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.collection.JavaConverters._

class DatastoreEdgeFetcher(graph: S2GraphLike,
                           datastore: Datastore) extends EdgeFetcher {

  import DatastoreStorage._

  lazy private val futureCache =
    new DeferCache[StepResult, Promise, Future](graph.config, StepResult.Empty, "DatastoreFutureCache", false)

  private def fetch(queryRequest: QueryRequest,
                    parentEdges: Seq[EdgeWithScore])(implicit ec: ExecutionContext): Future[StepResult] = {
    val queryParam = queryRequest.queryParam

    def fetchInner(query: com.spotify.asyncdatastoreclient.Query): Future[StepResult] = {
      asScala(datastore.executeAsync(query)).map { queryResult =>
        val edges = queryResult.getAll.asScala.map(toS2Edge(graph, _))

        // not support degree edges.
        val degreeEdges = Nil

        // not support cursor yet.
        val lastCursor = Nil

        StorageIO.toEdges(edges, queryRequest, parentEdges, degreeEdges, lastCursor, queryParam.offset, queryParam.limit)
      }
    }

    //TODO: toQuery should set up all query options property to datastore Query class.
    val query = toQuery(graph, queryRequest, parentEdges)

    if (queryParam.cacheTTLInMillis < 0) fetchInner(query)
    else {
      val fullCacheKey = queryRequest.query.fullCacheKey

      futureCache.getOrElseUpdate(fullCacheKey, queryParam.cacheTTLInMillis)(fetchInner(query))
    }
  }

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val futures = queryRequests.map { queryRequest =>
      val queryOption = queryRequest.query.queryOption
      val queryParam = queryRequest.queryParam
      val shouldBuildParents = queryOption.returnTree || queryParam.whereHasParent
      val parentEdges = if (shouldBuildParents) prevStepEdges.getOrElse(queryRequest.vertex.id, Nil) else Nil

      fetch(queryRequest, parentEdges)
    }

    Future.sequence(futures)
  }

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] = {
    val futures = Label.findAll().groupBy(_.hbaseTableName).toSeq.map { case (hTableName, labels) =>
      val distinctLabels = labels.toSet
      val kind = toKind(hTableName, EdgePostfix)

      asScala(datastore.executeAsync(toQuery(kind))).map { queryResult =>
        queryResult.getAll().asScala.map { entity =>
          toS2Edge(graph, entity)
        }.filter(e => distinctLabels(e.innerLabel))
      }
    }

    Future.sequence(futures).map(_.flatten)
  }
}
