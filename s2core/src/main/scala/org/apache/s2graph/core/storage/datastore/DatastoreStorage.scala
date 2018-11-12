package org.apache.s2graph.core.storage.datastore

import java.io.File
import java.util.function.{BiConsumer, Consumer}

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.spotify.asyncdatastoreclient._
import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.parsers._
import org.apache.s2graph.core.schema.{ColumnMeta, Label, LabelMeta}
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageSerDe
import org.apache.s2graph.core.storage.{Storage, StorageManagement, StorageSerDe}
import org.apache.s2graph.core.types.{InnerVal, InnerValLike, InnerValLikeWithTs, VertexId}
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object DatastoreStorage {
  val delimiter = "\u2980"
  val EdgePostfix = "e"
  val SnapshotEdgePostfix = "s"
  val VertexPostfix = "v"

  val ConnectionTimeoutKey = "connectionTimeout"
  val RequestTimeoutKey = "requestTimeout"
  val MaxConnectionsKey = "maxConnections"
  val RequestRetryKey = "requestRetry"

  val HostKey = "datastore.host"
  val ProjectKey = "datastore.dataset"
  val NamespaceKey = "datastore.namespace"
  val KeyPathKey = "datastore.keypath"
  val VersionKey = "datastore.version"

  def initDatastore(config: Config): Datastore = {
    val connectionTimeout = Try { config.getInt(ConnectionTimeoutKey) }.getOrElse(5000)
    val requestTimeout = Try { config.getInt(RequestTimeoutKey) }.getOrElse(1000)
    val maxConnections = Try { config.getInt(MaxConnectionsKey) }.getOrElse(100)
    val requestRetry = Try { config.getInt(RequestRetryKey) }.getOrElse(3)

    val host = Try { config.getString(HostKey) }.getOrElse("http://localhost:8080")
    val project = Try { config.getString(ProjectKey) }.getOrElse("async-test")
    val version = Try { config.getString(VersionKey) }.getOrElse("v1beta3")
    val namespace = Try { config.getString(NamespaceKey) }.getOrElse("test")
    val keyPathOpt = Try { config.getString(KeyPathKey) }.toOption

    val builder = DatastoreConfig.builder()
      .connectTimeout(connectionTimeout)
      .requestTimeout(requestTimeout)
      .maxConnections(maxConnections)
      .requestRetry(requestRetry)
      .version(version)
      .host(host)
      .project(project)
      .namespace(namespace)

    keyPathOpt.foreach { keyPath =>
      import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
      import com.spotify.asyncdatastoreclient.DatastoreConfig
      import java.io.FileInputStream
      import java.io.IOException
      try {
        val creds = new FileInputStream(new File(keyPath))
        builder.credential(GoogleCredential.fromStream(creds).createScoped(DatastoreConfig.SCOPES))
      } catch {
        case e: IOException =>
          System.err.println("Failed to load credentials " + e.getMessage)
          System.exit(1)
      }
    }

    Datastore.create(builder.build())
  }

  def toEdgeId(edge: S2EdgeLike): EdgeId = {
    val timestamp = if (edge.innerLabel.consistencyLevel == "strong") 0l else edge.ts
    //    EdgeId(srcVertex.innerId, tgtVertex.innerId, label(), "out", timestamp)
    val (srcColumn, tgtColumn) = edge.innerLabel.srcTgtColumn(edge.getDir())
    if (edge.getDir() == GraphUtil.directions("out"))
      EdgeId(VertexId(srcColumn, edge.srcVertex.id.innerId), VertexId(tgtColumn, edge.tgtVertex.id.innerId), edge.label(), "out", timestamp)
    else
      EdgeId(VertexId(tgtColumn, edge.tgtVertex.id.innerId), VertexId(srcColumn, edge.srcVertex.id.innerId), edge.label(), "in", timestamp)
  }

  def encodeEdgeKey(edge: S2EdgeLike): String = {
    Seq(edge.edgeId.toString, edge.getDirection()).mkString(delimiter)
//    Seq(edge.srcVertex.id.toString(), edge.label(), edge.getDirection(), edge.tgtVertex.id.toString()).mkString(delimiter)
  }

//  def decodeEdgeKey(s: String): (EdgeId, String) = {
//    val Array(edgeIdStr, dirStr) = s.split(delimiter)
//    val edgeId = EdgeId.fromString(edgeIdStr)
//    (edgeId, dirStr)
//  }

  def toKind(tableName: String, element: String): String = {
    Seq(tableName, element).mkString(delimiter)
  }

  def toKind(edge: S2EdgeLike): String = {
    toKind(edge.innerLabel.hbaseTableName, EdgePostfix)
  }

  def toKind(snapshotEdge: SnapshotEdge): String = {
    toKind(snapshotEdge.label.hbaseTableName, SnapshotEdgePostfix)
  }

  def toKind(vertex: S2VertexLike): String = {
    toKind(vertex.service.hTableName, VertexPostfix)
  }

  /**
    * translate s2graph's S2EdgeLike class to storage specific class that will be used for mutation request
    * in datastore, every mutation is involved with (Key, Entity) class,
    * so this method is intended to convert S2EdgeLike to Entity.
    *
    * @param edge
    * @return
    */
  def toEntity(edge: S2EdgeLike): Entity = {
    //TODO: only index property that is used in any LabelIndex.
    val edgeKey = Key.builder(toKind(edge), encodeEdgeKey(edge)).build()

    val builder = Entity.builder(edgeKey)
      .property("version", edge.version)
      .property(LabelMeta.from.name, edge.srcVertex.id.toString())
      .property(LabelMeta.to.name, edge.tgtVertex.id.toString())
      .property("label", edge.label())
      .property("direction", edge.getDirection())
      .property(LabelMeta.timestamp.name, edge.getTs())

    edge.properties().forEachRemaining(new Consumer[Property[_]] {
      override def accept(t: Property[_]): Unit = {
        builder.property(t.key(), t.value())
      }
    })

    builder.build()
  }

  def toEntity(snapshotEdge: SnapshotEdge): Entity = {
    val edgeKey = Key.builder(toKind(snapshotEdge), snapshotEdge.edge.edgeId.toString).build()

    val builder = Entity.builder(edgeKey)
        .property("version", snapshotEdge.version)
        .property("dir", snapshotEdge.dir)
        .property("op", snapshotEdge.op.toInt)

    snapshotEdge.propsWithTs.forEach(new BiConsumer[String, Property[_]] {
      override def accept(key: String, t: Property[_]): Unit = {
        builder.property(t.key(), t.value())
      }
    })

    builder.build()
  }
  /**
    * translate s2graph's S2VertexLike class to storage specific class that will be used for mutation request
    * in datastore, every mutation is involved with (Key, Entity) class,
    * so this method is intended to convert S2VertexLike to Entity.
    *
    * @param vertex
    * @return
    */
  def toEntity(vertex: S2VertexLike): Entity = {
    val vertexKey = Key.builder(toKind(vertex), vertex.id.toString()).build()
    val builder = Entity.builder(vertexKey)

    vertex.properties().forEachRemaining(new Consumer[VertexProperty[_]]{
      override def accept(t: VertexProperty[_]): Unit = {
        builder.property(t.key(), t.value())
      }
    })

    builder.build()
  }

  /**
    * build storage implementation specific mutation request from a given S2VertexLike.
    * client to storage will exploit this to build mutation request.
    *
    * @param vertex
    * @return
    */
  def toMutationStatement(vertex: S2VertexLike): MutationStatement = {
    val vertexEntity = toEntity(vertex)
    //TODO: add implementation for all operations.
    vertex.op match {
      case 0 => // insert
        QueryBuilder.update(vertexEntity).upsert()
      case 1 => // update
        QueryBuilder.update(vertexEntity).upsert()
      case 2 => // increment
        throw new IllegalArgumentException("increment is not supported on vertex.")
      case 3 => // delete
        QueryBuilder.delete(vertexEntity.getKey)
      case 4 => // deleteAll
        QueryBuilder.delete(vertexEntity.getKey)
      case 5 => // insertBulk
        QueryBuilder.update(vertexEntity).upsert()
      case 6 => // incrementCount
        throw new IllegalArgumentException("incrementCount not supported on vertex.")
      case _ => throw new IllegalArgumentException(s"$vertex operation ${vertex.op} is not supported.")
    }
  }

  /**
    * build storage implementation specific mutation request from a given S2EdgeLike.
    * client to storage will exploit this to build mutation request.
    *
    * @param edge
    * @return
    */
  def toMutationStatement(edge: S2EdgeLike): MutationStatement = {
    val edgeEntity = toEntity(edge)
    //TODO: add implementation for all operations.
    edge.getOp() match {
      case 0 => // insert
        QueryBuilder.update(edgeEntity).upsert()
      case 1 => // update
        QueryBuilder.update(edgeEntity).upsert()
      case 2 => // increment
        throw new IllegalArgumentException(s"increment on edge is not yet supported.")
      case 3 => // delete
        QueryBuilder.delete(edgeEntity.getKey)
      case 4 => // deleteAll
        throw new IllegalArgumentException(s"deleteAll on edge is not yet supported.")
      //        QueryBuilder.delete(parentEntity.getKey())
      case 5 => // insertBulk
        QueryBuilder.update(edgeEntity).upsert()
      case 6 => //incrementCount
        throw new IllegalArgumentException(s"incrementCount on edge is not yet supported.")
      case _ => throw new IllegalArgumentException(s"$edge operation ${edge.op} is not supported.")
    }
  }

  def toMutationStatement(snapshotEdge: SnapshotEdge): MutationStatement = {
    val edgeEntity = toEntity(snapshotEdge)
    QueryBuilder.insert(edgeEntity)
  }

  def toBatch(edge: S2EdgeLike, batch: Batch): Unit = {
    edge.relatedEdges.map { edge =>
      val mutation = toMutationStatement(edge)

      batch.add(mutation)
    }
  }

  def toBatch(edge: S2EdgeLike): Batch = {
    val batch = QueryBuilder.batch()

    toBatch(edge, batch)

    batch
  }

  def toQuery(snapshotEdge: SnapshotEdge): com.spotify.asyncdatastoreclient.KeyQuery = {
    QueryBuilder.query(toKind(snapshotEdge), snapshotEdge.edge.edgeId.toString)
  }

  def toQuery(edge: S2EdgeLike): com.spotify.asyncdatastoreclient.KeyQuery = {
    QueryBuilder.query(toKind(edge), encodeEdgeKey(edge))
  }
  /**
    * build storage implementation specific query request from S2Graph's QueryRequest.
    *
    * @param queryRequest
    * @return
    */
  def toQuery(queryRequest: QueryRequest): com.spotify.asyncdatastoreclient.Query = {
    val queryOption = queryRequest.query.queryOption
    val qp = queryRequest.queryParam
    val label = qp.label

    val queryBuilder = QueryBuilder.query().kindOf(toKind(label.hbaseTableName, EdgePostfix)).limit(qp.limit)

    toFilterBys(queryRequest).foreach(queryBuilder.filterBy)
    toOrderBys(queryOption).foreach(queryBuilder.orderBy)
    //TODO: currently group by is not supported.
//    toGroupBys(queryOption).foreach(queryBuilder.groupBy)

    //TODO: not sure how to implement offset, cursor, limit yet.
    queryBuilder.limit(qp.limit)
  }

  def toQuery(kind: String): com.spotify.asyncdatastoreclient.Query = {
    QueryBuilder.query().kindOf(kind)
  }

  def toFilterBys(queryRequest: QueryRequest): Seq[com.spotify.asyncdatastoreclient.Filter] = {
    val qp = queryRequest.queryParam
    val label = qp.label
    val dir = qp.dir.toInt
    // base filter
    val baseFilters = Seq(
      QueryBuilder.eq(LabelMeta.from.name, queryRequest.vertex.id.toString()),
      QueryBuilder.eq("direction", qp.direction)
    )
    // duration
    val durationFilters = qp.durationOpt.map { case (minTs, maxTs) =>
      Seq(
        QueryBuilder.gt(LabelMeta.timestamp.name, minTs),
        QueryBuilder.lt(LabelMeta.timestamp.name, maxTs)
      )
    }.getOrElse(Nil)

    // followings are filter operators supported by datastore.
//    LESS_THAN,
//    LESS_THAN_OR_EQUAL,
//    GREATER_THAN,
//    GREATER_THAN_OR_EQUAL,
//    EQUAL,
//    HAS_ANCESTOR
    // TODO: change value type on Clause class to be AnyRef instead of String.
    // TODO: parent property filter need to be considered too.
    val optionalFilters = Nil
//    qp.where.get.clauses.map { clause =>
//      clause match {
//        case lt: Lt => QueryBuilder.lt(lt.propKey, lt.anyValueToCompare(label, dir, lt.propKey, lt.value))
//        case gt: Gt => QueryBuilder.gt(gt.propKey, gt.anyValueToCompare(label, dir, gt.propKey, gt.value))
//        case eq: Eq => QueryBuilder.eq(eq.propKey, eq.anyValueToCompare(label, dir, eq.propKey, eq.value))
//        case _ => throw new IllegalArgumentException(s"lt, gt, eq are only supported currently.")
//      }
//    }

    baseFilters ++ durationFilters ++ optionalFilters
  }

  def toOrderBys(queryOption: QueryOption): Seq[com.spotify.asyncdatastoreclient.Order] = {
    if (queryOption.orderByKeys.isEmpty) {
      // default timestamp.
      Seq(QueryBuilder.desc(LabelMeta.timestamp.name))
    } else {
      queryOption.orderByKeys.map { key =>
        QueryBuilder.desc(key)
      }
    }
  }

  def toGroupBys(queryOption: QueryOption): Seq[com.spotify.asyncdatastoreclient.Group] = {
    queryOption.groupBy.keys.map { key =>
      QueryBuilder.group(key)
    }
  }

  /**
    * translate storage specific data(in datastore, it is encapsulated in Value class) into S2Graph's InnerVal class.
    *
    * @param dataType
    * @param schemaVer
    * @param value
    * @return
    */
  def toInnerVal(dataType: String,
                 schemaVer: String,
                 value: Value): InnerValLike = {
    dataType match {
      case "string" => InnerVal.withStr(value.getString, schemaVer)
      case "boolean" => InnerVal.withBoolean(value.getBoolean, schemaVer)
      case "integer" => InnerVal.withInt(value.getInteger.toInt, schemaVer)
      case "long" => InnerVal.withLong(value.getInteger, schemaVer)
      case "double" => InnerVal.withDouble(value.getDouble, schemaVer)
      case "float" => InnerVal.withFloat(value.getDouble.toFloat, schemaVer)
      case _ => throw new IllegalStateException(s"$dataType data type is illegal.")
    }
  }

  /**
    * given storage specific class that hold query result(data, in datastore, it is encapsulated in Entity),
    * extract all properties on fetched result then build property map that will be attached into S2EdgeLike.
    *
    * @param label
    * @param ts
    * @param entity
    * @return
    */
  def parseProps(label: Label,
                 ts: Long,
                 entity: Entity): Map[LabelMeta, InnerValLikeWithTs] = {
    val props = mutable.Map.empty[LabelMeta, InnerValLikeWithTs]
    val schemaVer = label.schemaVersion

    // S2Edge expect _timestamp must exist in props as assert.
    props += (LabelMeta.timestamp -> InnerValLikeWithTs(InnerVal.withLong(ts, schemaVer), ts))

    entity.getProperties.forEach(new BiConsumer[String, Value] {
      override def accept(key: String, value: Value): Unit = {
        label.validLabelMetasInvMap.get(key).foreach { labelMeta =>
          val innerVal = toInnerVal(labelMeta.dataType, schemaVer, value)

          props += (labelMeta -> InnerValLikeWithTs(innerVal, ts))
        }
      }
    })

    props.toMap
  }

  def toSnapshotEdge(graph: S2GraphLike,
                     entity: Entity): SnapshotEdge = {
    val builder = graph.elementBuilder
    val edgeId = EdgeId.fromString(entity.getKey.getName)
    val label = Label.findByName(edgeId.labelName).getOrElse(throw new IllegalStateException(s"$entity has invalid label"))

    val srcVertex = builder.newVertex(edgeId.srcVertexId)
    val tgtVertex = builder.newVertex(edgeId.tgtVertexId)
    val dir = entity.getInteger("dir").toInt
    val op = entity.getInteger("op").toByte
    val version = entity.getInteger("version")
    val ts = entity.getInteger(LabelMeta.timestamp.name)

    val props = parseProps(label, ts, entity)

    val snapshotEdge = SnapshotEdge(graph, srcVertex, tgtVertex, label, dir, op, version, S2Edge.EmptyProps, None, 0, None, None)
    S2Edge.fillPropsWithTs(snapshotEdge, props)
    snapshotEdge
  }
  /**
    * translate storage specific class that hold data into S2EdgeLike.
    *
    * @param graph
    * @param entity
    * @return
    */
  def toS2Edge(graph: S2GraphLike,
               entity: Entity): S2EdgeLike = {
    val builder = graph.elementBuilder
    val label = Label.findByName(entity.getString("label")).getOrElse(throw new IllegalStateException(s"$entity has invalid label"))

    val version = entity.getInteger("version")
    val srcVertexId = VertexId.fromString(entity.getString(LabelMeta.from.name))
    val tgtVertexId = VertexId.fromString(entity.getString(LabelMeta.to.name))
    val ts = entity.getInteger(LabelMeta.timestamp.name)
    val direction = entity.getString("direction")

    val props = parseProps(label, ts, entity)

    builder.newEdge(
      builder.newVertex(srcVertexId),
      builder.newVertex(tgtVertexId),
      label,
      dir = GraphUtil.toDirection(direction),
      version = version,
      propsWithTs = props,
      parentEdges = Nil,
      originalEdgeOpt = None,
      pendingEdgeOpt = None,
      statusCode = 0,
      lockTs = None,
      tsInnerValOpt = Option(InnerVal.withLong(ts, label.schemaVersion))
    )
  }

  /**
    * translate storage specific class that hold data into S2VertexLike.
    *
    * @param graph
    * @param entity
    * @return
    */
  def toS2Vertex(graph: S2GraphLike,
                 entity: Entity): S2VertexLike = {
    val vertexId = VertexId.fromString(entity.getKey.getName)
    val schemaVer = vertexId.column.schemaVersion

    val builder = graph.elementBuilder
    val v = builder.newVertex(vertexId)

    entity.getProperties.forEach(new BiConsumer[String, Value] {
      override def accept(key: String, value: Value): Unit = {
        vertexId.column.validColumnMetasInvMap.get(key).foreach { columnMeta =>
          val innerVal = toInnerVal(columnMeta.dataType, schemaVer, value)

          v.propertyInner(Cardinality.single, key, innerVal.value)
        }
      }
    })

    v
  }

  /**
    * Since com.spotify.asyncdatastoreclient.datastore client return ListenableFuture,
    * use this helper method to translate ListenableFuture to scala.concurrent.Future.
    *
    * @param lf
    * @param ec
    * @tparam V
    * @return
    */
  def asScala[V](lf: ListenableFuture[V])(implicit ec: ExecutionContext): Future[V] = {
    val p = Promise[V]

    Futures.addCallback(lf, new FutureCallback[V] {
      def onFailure(t: Throwable): Unit = p failure t
      def onSuccess(result: V): Unit    = p success result
    })

    p.future
  }

}

class DatastoreStorage(graph: S2GraphLike, config: Config) extends Storage(graph, config) {
  import DatastoreStorage._
  val datastore: Datastore = initDatastore(config)

  override val management: StorageManagement = new DatastoreStorageManagement(datastore)

  //TODO: this store does not use serDe variable, but interface force us to initialize serDe.
  override val serDe: StorageSerDe = new AsynchbaseStorageSerDe(graph)

  override val edgeFetcher: EdgeFetcher = new DatastoreEdgeFetcher(graph, datastore)
  override val vertexFetcher: VertexFetcher = new DatastoreVertexFetcher(graph, datastore)
  override val edgeMutator: EdgeMutator = new DatastoreEdgeMutator(graph, datastore)
  override val vertexMutator: VertexMutator = new DatastoreVertexMutator(graph, datastore)
}
