package org.apache.s2graph.core.storage.datastore


import java.util.function.{BiConsumer, Consumer}

import com.spotify.asyncdatastoreclient._
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.types.VertexId
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.concurrent.{ExecutionContext, Future}

object DatastoreVertexMutator {
  def fromEntity(graph: S2GraphLike,
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

}

class DatastoreVertexMutator(graph: S2GraphLike,
                             dsService: Datastore) extends VertexMutator {

  import DatastoreVertexFetcher._
  import DatastoreVertexMutator._
  // pool of datastores and lookup by zkQuorum?

  override def mutateVertex(zkQuorum: String,
                            vertex: S2VertexLike,
                            withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] = {
    val hTableName = vertex.hbaseTableName
    val mutationStatement = vertex.op match {
      case 0 => // insert
        val insert = QueryBuilder.insert(hTableName, vertex.id.toString)
        vertex.properties().forEachRemaining(new Consumer[VertexProperty[_]] {
          override def accept(vp: VertexProperty[_]): Unit = {
            insert.value(vp.key(), vp.value())
          }
        })
        insert
      case 1 => // update
        val update = QueryBuilder.update(hTableName, vertex.id.toString())
        vertex.properties().forEachRemaining(new Consumer[VertexProperty[_]] {
          override def accept(vp: VertexProperty[_]): Unit = {
            update.value(vp.key(), vp.value())
          }
        })
        update
      case 2 => // increment
        throw new IllegalArgumentException("increment is not supported on vertex.")
      case 3 => // delete
        QueryBuilder.delete(hTableName, vertex.id.toString())
      case 4 => // deleteAll
        QueryBuilder.delete(hTableName, vertex.id.toString())
      case 5 => // insertBulk
        val insert = QueryBuilder.insert(hTableName, vertex.id.toString)
        vertex.properties().forEachRemaining(new Consumer[VertexProperty[_]] {
          override def accept(vp: VertexProperty[_]): Unit = {
            insert.value(vp.key(), vp.value())
          }
        })
        insert
      case 6 => // incrementCount
        throw new IllegalArgumentException("incrementCount not supported on vertex.")
      case _ => throw new IllegalArgumentException(s"$vertex operation ${vertex.op} is not supported.")
    }

    asScala(dsService.executeAsync(mutationStatement)).map { _ =>
      MutateResponse.Success
    }
  }
}
