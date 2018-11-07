package org.apache.s2graph.core.storage.datastore

import java.util.function.BiConsumer

import com.spotify.asyncdatastoreclient._
import org.apache.s2graph.core.fetcher.BaseFetcherTest
import org.apache.s2graph.core.{QueryParam, QueryRequest, S2VertexProperty, VertexQueryParam, Query => S2Query}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class DatastoreVertexTest extends BaseFetcherTest {
  implicit val ec = ExecutionContext.global
  val DATASTORE_HOST: String = System.getProperty("host", "http://localhost:8080")
  val PROJECT: String = System.getProperty("dataset", "async-test")
  val NAMESPACE: String = System.getProperty("namespace", "test")
  val KEY_PATH: String = System.getProperty("keypath")
  val VERSION: String = System.getProperty("version", "v1beta3")

  val serviceName = "test"
  val columnName = "user"
  val labelName = "test_label"

  var datastore: Datastore = _

  val datastoreConfig = DatastoreConfig.builder()
    .connectTimeout(5000)
    .requestTimeout(1000)
    .maxConnections(5)
    .requestRetry(3)
    .version(VERSION)
    .host(DATASTORE_HOST)
    .project(PROJECT)
    .namespace(NAMESPACE)
    .build()

  override def beforeAll: Unit = {
    super.beforeAll

    datastore = Datastore.create(datastoreConfig)
    initEdgeFetcher(serviceName, columnName, labelName, None)
  }

  override def afterAll(): Unit = {
    super.afterAll()

    removeAll()
    datastore.close()
  }

  private def removeAll(): Unit = {
    val queryAll = QueryBuilder.query.keysOnly
    import scala.collection.JavaConversions._
    for (entity <- datastore.execute(queryAll)) {
      datastore.execute(QueryBuilder.delete(entity.getKey))
    }
  }

  test("test vertex.") {
    val builder = graph.elementBuilder
    val vertexId = builder.newVertexId(serviceName)(columnName)("a")
    val vertex = builder.newVertex(vertexId)
    vertex.propertyInner(Cardinality.single, "name", "xxx")

    val mutator = new DatastoreVertexMutator(graph, datastore)
    val fetcher = new DatastoreVertexFetcher(graph, datastore)

    val mutateFuture = mutator.mutateVertex("zk", vertex, true)
    Await.result(mutateFuture, Duration("10 seconds"))

    val vqp = VertexQueryParam(Seq(vertexId))
    val fetchFuture = fetcher.fetchVertices(vqp)
    val fetchedVertices = Await.result(fetchFuture, Duration("10 seconds"))
    fetchedVertices.foreach { v =>
      println(v)
      v.props.forEach(new BiConsumer[String, S2VertexProperty[_]] {
        override def accept(t: String, u: S2VertexProperty[_]): Unit = {
          println(s"key = ${t}, value = ${u.value}")
        }
      })
    }
  }

  test("test edge.") {
    val builder = graph.elementBuilder
    val vertexId = builder.newVertexId(serviceName)(columnName)("user_1")
    val vertex = builder.newVertex(vertexId)

    val edge1 = builder.toEdge("user_1", "user_z", labelName, "out", Map("score" -> 0.1), operation = "insert")
    val edge2 = builder.toEdge("user_1", "user_x", labelName, "out", Map("score" -> 0.8), operation = "insert")

    val mutator = new DatastoreEdgeMutator(graph, datastore)
    val fetcher = new DatastoreEdgeFetcher(graph, datastore)

    val mutateFuture = mutator.mutateWeakEdges("zk", Seq(edge1, edge2), true)
    Await.result(mutateFuture, Duration("10 seconds"))

    val queryParam = QueryParam(labelName = labelName)

    val queryRequest = QueryRequest(S2Query.empty, 0, vertex, queryParam)
    val fetchFuture = fetcher.fetches(Seq(queryRequest), Map.empty)
    Await.result(fetchFuture, Duration("10 seconds")).foreach { stepResult =>
      stepResult.edgeWithScores.foreach { es =>
        println(s"${es.edge}")
      }
    }
  }
}
