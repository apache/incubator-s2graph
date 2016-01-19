package com.kakao.s2graph.core.storage

import com.google.common.cache.Cache
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{Service, Label}
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.Config

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

abstract class Storage(val config: Config)(implicit ec: ExecutionContext) {

  def cacheOpt: Option[Cache[Integer, Seq[QueryResult]]]

  def vertexCacheOpt: Option[Cache[Integer, Option[Vertex]]]

  // Serializer/Deserializer
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): StorageSerializable[SnapshotEdge]

  def indexEdgeSerializer(indexedEdge: IndexEdge): StorageSerializable[IndexEdge]

  def vertexSerializer(vertex: Vertex): StorageSerializable[Vertex]

  def snapshotEdgeDeserializer: StorageDeserializable[SnapshotEdge]

  def indexEdgeDeserializer: StorageDeserializable[IndexEdge]

  def vertexDeserializer: StorageDeserializable[Vertex]

  // Interface
  def getEdges(q: Query): Future[Seq[QueryRequestWithResult]]

  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]]

  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]]

  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {

    val edgeBuffer = ArrayBuffer[Edge]()
    val vertexBuffer = ArrayBuffer[Vertex]()

    elements.foreach {
      case e: Edge => edgeBuffer += e
      case v: Vertex => vertexBuffer += v
      case any@_ => logger.error(s"Unknown type: ${any}")
    }

    val edgeFuture = mutateEdges(edgeBuffer, withWait)
    val vertexFuture = mutateVertices(vertexBuffer, withWait)

    val graphFuture = for {
      edgesMutated <- edgeFuture
      verticesMutated <- vertexFuture
    } yield edgesMutated ++ verticesMutated

    graphFuture
  }


  def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean]

  def mutateEdges(edges: Seq[Edge], withWait: Boolean = false): Future[Seq[Boolean]]

//  def mutateEdges(edges: Seq[Edge],
//                  withWait: Boolean = false): Future[Seq[Boolean]] = {
//    val futures = edges.map { edge => mutateEdge(edge, withWait) }
//    Future.sequence(futures)
//  }

  def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean]

  def mutateVertices(vertices: Seq[Vertex],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {
    val futures = vertices.map { vertex => mutateVertex(vertex, withWait) }
    Future.sequence(futures)
  }

  def deleteAllAdjacentEdges(srcVertices: Seq[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean]

  def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]]

  def flush(): Unit

  def createTable(zkAddr: String,
                  tableName: String,
                  cfs: List[String],
                  regionMultiplier: Int,
                  ttl: Option[Int],
                  compressionAlgorithm: String): Unit

  def toEdge[K: CanSKeyValue](kv: K,
                              queryParam: QueryParam,
                              cacheElementOpt: Option[IndexEdge],
                              parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
    try {
      val indexEdge = indexEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)

      Option(indexEdge.toEdge.copy(parentEdges = parentEdges))
    } catch {
      case ex: Exception =>
        logger.error(s"Fail on toEdge: ${kv.toString}, ${queryParam}")
        None
    }
  }

  def toSnapshotEdge[K: CanSKeyValue](kv: K,
                                      queryParam: QueryParam,
                                      cacheElementOpt: Option[SnapshotEdge] = None,
                                      isInnerCall: Boolean,
                                      parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
    val snapshotEdge = snapshotEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)

    if (isInnerCall) {
      val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
      if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
      else None
    } else {
      if (Edge.allPropsDeleted(snapshotEdge.props)) None
      else {
        val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
        if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
        else None
      }
    }
  }

  def toEdges[K: CanSKeyValue](kvs: Seq[K],
                               queryParam: QueryParam,
                               prevScore: Double = 1.0,
                               isInnerCall: Boolean,
                               parentEdges: Seq[EdgeWithScore]): Seq[EdgeWithScore] = {
    if (kvs.isEmpty) Seq.empty
    else {
      val first = kvs.head
      val kv = first
      val cacheElementOpt =
        if (queryParam.isSnapshotEdge) None
        else Option(indexEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, None))

      for {
        kv <- kvs
        edge <-
        if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryParam, None, isInnerCall, parentEdges)
        else toEdge(kv, queryParam, cacheElementOpt, parentEdges)
      } yield {
        //TODO: Refactor this.
        val currentScore =
          queryParam.scorePropagateOp match {
            case "plus" => edge.rank(queryParam.rank) + prevScore
            case _ => edge.rank(queryParam.rank) * prevScore
          }
        EdgeWithScore(edge, currentScore)
      }
    }
  }


}

