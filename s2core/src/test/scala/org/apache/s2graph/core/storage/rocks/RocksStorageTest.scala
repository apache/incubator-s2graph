package org.apache.s2graph.core.storage.rocks

import org.apache.s2graph.core.TestCommonWithModels
import org.apache.s2graph.core.mysqls.{Service, ServiceColumn}
import org.apache.tinkerpop.gremlin.structure.T
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._

class RocksStorageTest  extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

  test("VertexTest: shouldNotGetConcurrentModificationException()") {
    val service = Service.findByName(serviceName, useCache = false).getOrElse {
      throw new IllegalStateException("service not found.")
    }
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse {
      throw new IllegalStateException("column not found.")
    }

    val vertexId = graph.newVertexId(service, column, 1L)

    val vertex = graph.newVertex(vertexId)
    for (i <- (0 until 10)) {
      vertex.addEdge(labelName, vertex)
    }

    println(graph.edges().toSeq)
    println("*" * 100)
    vertex.remove()
    println(graph.vertices().toSeq)
  }
}
