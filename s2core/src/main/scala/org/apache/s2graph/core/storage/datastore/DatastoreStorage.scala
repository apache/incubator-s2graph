package org.apache.s2graph.core.storage.datastore

import java.util.function.{BiConsumer, Consumer}

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.spotify.asyncdatastoreclient._
import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.parsers._
import org.apache.s2graph.core.schema.{Label, LabelMeta}
import org.apache.s2graph.core.types.{InnerVal, InnerValLike, InnerValLikeWithTs, VertexId}
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

object DatastoreStorage {
  /**
    * translate s2graph's S2EdgeLike class to storage specific class that will be used for mutation request
    * in datastore, every mutation is involved with (Key, Entity) class,
    * so this method is intended to convert S2EdgeLike to Entity.
    *
    * @param edge
    * @return
    */
  def toEntity(edge: S2EdgeLike): Entity = {
    val label = edge.innerLabel
    val edgeKey = Key.builder(label.hbaseTableName, edge.edgeId.toString).build()

    val builder = Entity.builder(edgeKey)
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

  /**
    * translate s2graph's S2VertexLike class to storage specific class that will be used for mutation request
    * in datastore, every mutation is involved with (Key, Entity) class,
    * so this method is intended to convert S2VertexLike to Entity.
    *
    * @param vertex
    * @return
    */
  def toEntity(vertex: S2VertexLike): Entity = {
    val vertexKey = Key.builder(vertex.hbaseTableName, vertex.id.toString()).build()
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
        QueryBuilder.insert(vertexEntity)
      case 1 => // update
        QueryBuilder.update(vertexEntity)
      case 2 => // increment
        throw new IllegalArgumentException("increment is not supported on vertex.")
      case 3 => // delete
        QueryBuilder.delete(vertexEntity.getKey)
      case 4 => // deleteAll
        QueryBuilder.delete(vertexEntity.getKey)
      case 5 => // insertBulk
        QueryBuilder.insert(vertexEntity)
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
        QueryBuilder.insert(edgeEntity)

      case 1 => // update
        QueryBuilder.update(edgeEntity)
      case 2 => // increment
        throw new IllegalArgumentException(s"increment on edge is not yet supported.")
      case 3 => // delete
        QueryBuilder.delete(edgeEntity.getKey)
      case 4 => // deleteAll
        throw new IllegalArgumentException(s"deleteAll on edge is not yet supported.")
      //        QueryBuilder.delete(parentEntity.getKey())
      case 5 => // insertBulk
        QueryBuilder.insert(edgeEntity)
      case 6 => //incrementCount
        throw new IllegalArgumentException(s"incrementCount on edge is not yet supported.")
      case _ => throw new IllegalArgumentException(s"$edge operation ${edge.op} is not supported.")
    }
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

    val queryBuilder = QueryBuilder.query().kindOf(label.hbaseTableName).limit(qp.limit)

    toFilterBys(queryRequest).foreach(queryBuilder.filterBy)
    toOrderBys(queryOption).foreach(queryBuilder.orderBy)
    //TODO: currently group by is not supported.
//    toGroupBys(queryOption).foreach(queryBuilder.groupBy)

    //TODO: not sure how to implement offset, cursor, limit yet.
    queryBuilder.limit(qp.limit)
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
    val optionalFilters = qp.where.get.clauses.map { clause =>
      clause match {
        case lt: Lt => QueryBuilder.lt(lt.propKey, lt.anyValueToCompare(label, dir, lt.propKey, lt.value))
        case gt: Gt => QueryBuilder.gt(gt.propKey, gt.anyValueToCompare(label, dir, gt.propKey, gt.value))
        case eq: Eq => QueryBuilder.eq(eq.propKey, eq.anyValueToCompare(label, dir, eq.propKey, eq.value))
        case _ => throw new IllegalArgumentException(s"lt, gt, eq are only supported currently.")
      }
    }

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
    * note that LabelMeta, schemaVersion are required to decide dataType for specific data.
    *
    * @param labelMeta
    * @param schemaVer
    * @param value
    * @return
    */
  def toInnerVal(labelMeta: LabelMeta,
                 schemaVer: String,
                 value: Value): InnerValLike = {
    labelMeta.dataType match {
      case "string" => InnerVal.withStr(value.getString, schemaVer)
      case "boolean" => InnerVal.withBoolean(value.getBoolean, schemaVer)
      case "integer" => InnerVal.withInt(value.getInteger.toInt, schemaVer)
      case "long" => InnerVal.withLong(value.getInteger, schemaVer)
      case "double" => InnerVal.withDouble(value.getDouble, schemaVer)
      case "float" => InnerVal.withFloat(value.getDouble.toFloat, schemaVer)
      case _ => throw new IllegalStateException(s"$labelMeta data type is illegal.")
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

    entity.getProperties.asScala.foreach { case (key, value) =>
      key match {
        case "label" =>
        case LabelMeta.from.name =>
        case LabelMeta.to.name =>
        case LabelMeta.timestamp.name =>
        case _ =>
          label.metaPropsInvMap.get(key).foreach { labelMeta =>
            val innerVal = toInnerVal(labelMeta, schemaVer, value)

            props += (labelMeta -> InnerValLikeWithTs(innerVal, ts))
          }
      }
    }

    props.toMap
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

    val srcVertexId = VertexId.fromString(entity.getString(LabelMeta.from.name))
    val tgtVertexId = VertexId.fromString(entity.getString(LabelMeta.to.name))
    val ts = entity.getInteger(LabelMeta.timestamp.name)

    val props = parseProps(label, ts, entity)

    builder.newEdge(
      builder.newVertex(srcVertexId),
      builder.newVertex(tgtVertexId),
      label,
      dir = GraphUtil.toDirection("out"),
      version = ts,
      propsWithTs = props
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
    val builder = graph.elementBuilder
    val v = builder.newVertex(vertexId)

    entity.getProperties.forEach(new BiConsumer[String, AnyRef] {
      override def accept(key: String, value: AnyRef): Unit = {
        v.propertyInner(Cardinality.single, key, value)
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
class DatastoreStorage(graph: S2GraphLike, config: Config) {
  import DatastoreStorage._
}
