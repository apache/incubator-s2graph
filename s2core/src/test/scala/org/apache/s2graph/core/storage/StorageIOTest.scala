package org.apache.s2graph.core.storage

import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorageSerDe
import org.apache.s2graph.core.storage.rocks.RocksStorageSerDe
import org.apache.s2graph.core.storage.serde.{StorageDeserializable, StorageSerializable}
import org.apache.s2graph.core.{S2Vertex, TestCommonWithModels}
import org.scalatest.{FunSuite, Matchers}

class StorageIOTest extends FunSuite with Matchers with TestCommonWithModels {

  initTests()

  test("AsynchbaseStorageIO: VertexSerializer/Deserializer") {
    def check(vertex: S2Vertex,
              op: S2Vertex => StorageSerializable[S2Vertex],
              deserializer: StorageDeserializable[S2Vertex]): Boolean = {
      val sKeyValues = op(vertex).toKeyValues
      val deserialized = deserializer.fromKeyValues(sKeyValues, None)
      vertex == deserialized
    }

    val serDe: StorageSerDe = new AsynchbaseStorageSerDe(graph)
    val service = Service.findByName(serviceName, useCache = false).getOrElse {
      throw new IllegalStateException("service not found.")
    }
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse {
      throw new IllegalStateException("column not found.")
    }

    val vertexId = graph.newVertexId(service, column, 1L)
    val vertex = graph.newVertex(vertexId)

    check(vertex, serDe.vertexSerializer, serDe.vertexDeserializer(vertex.serviceColumn.schemaVersion))
  }

  test("RocksStorageIO: VertexSerializer/Deserializer") {
    def check(vertex: S2Vertex,
              op: S2Vertex => StorageSerializable[S2Vertex],
              deserializer: StorageDeserializable[S2Vertex]): Boolean = {
      val sKeyValues = op(vertex).toKeyValues
      val deserialized = deserializer.fromKeyValues(sKeyValues, None)
      vertex == deserialized
    }

    val serDe: StorageSerDe = new RocksStorageSerDe(graph)
    val service = Service.findByName(serviceName, useCache = false).getOrElse {
      throw new IllegalStateException("service not found.")
    }
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse {
      throw new IllegalStateException("column not found.")
    }

    val vertexId = graph.newVertexId(service, column, 1L)
    val vertex = graph.newVertex(vertexId)

    check(vertex, serDe.vertexSerializer, serDe.vertexDeserializer(vertex.serviceColumn.schemaVersion))
  }
}
