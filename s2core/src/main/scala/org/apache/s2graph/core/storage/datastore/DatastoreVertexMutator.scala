package org.apache.s2graph.core.storage.datastore


import java.util.function.{BiConsumer, Consumer}

import com.google.appengine.api.datastore._
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.types.{InnerVal, VertexId}
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.concurrent.{ExecutionContext, Future, Promise}

object DatastoreVertexMutator {
  val kind = "vertex"

  def toKeys(vertexIds: Seq[VertexId]): Seq[Key] = {
    vertexIds.map(toKey)
  }

  def toKey(vertexId: VertexId): Key = {
    KeyFactory.createKey(kind, vertexId.toString())
  }

  def toEntity(vertex: S2VertexLike, key: Key): Entity = {
    val entity = new Entity(key)

    vertex.properties().forEachRemaining(new Consumer[VertexProperty[_]] {
      override def accept(vp: VertexProperty[_]): Unit = {
        val s2vp = vp.asInstanceOf[S2VertexProperty[_]]
        entity.setProperty(s2vp.key, s2vp.value)
      }
    })

    entity
  }

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

  def mergeEntity(cur: Entity, prev: Entity): Unit = {
    prev.getProperties.forEach(new BiConsumer[String, AnyRef] {
      override def accept(name: String, v: AnyRef): Unit = {
        if (!cur.hasProperty(name)) {
          cur.setProperty(name, v)
        }
      }
    })
  }
}
class DatastoreVertexMutator(dsService: DatastoreService) extends VertexMutator {
  import DatastoreVertexMutator._

  override def mutateVertex(zkQuorum: String,
                            vertex: S2VertexLike,
                            withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] = {

    val key = toKey(vertex.id)
    val entity = toEntity(vertex, key)

    vertex.op match {
      case 0 => // insert
        dsService.put(entity)
      case 1 => // update
        val tx = dsService.beginTransaction()
        Option(dsService.get(key)).foreach { prevEntity =>
          mergeEntity(entity, prevEntity)
        }
        dsService.put(entity)
        tx.commit()
      case 2 => // increment
      case 3 => // delete
        dsService.delete(key)
      case 4 => // deleteAll
        dsService.delete(key)
      case 5 => // insertBulk
//        val batch = datastore.newBatch()
//        batch.add(entity)
//        batch.submit()
        dsService.put(entity)
      case 6 => // incrementCount
      case _ => throw new IllegalArgumentException(s"$vertex operation ${vertex.op} is not supported.")
    }

    Future.successful(MutateResponse.Success)
  }
}
