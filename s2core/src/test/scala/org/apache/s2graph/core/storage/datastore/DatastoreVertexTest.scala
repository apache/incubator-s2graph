package org.apache.s2graph.core.storage.datastore

import java.util.function.{BiConsumer, Consumer}

import com.spotify.asyncdatastoreclient._
import org.apache.s2graph.core.{S2Vertex, S2VertexProperty, VertexQueryParam}
import org.apache.s2graph.core.fetcher.BaseFetcherTest
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

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

    val vertexMutator = new DatastoreVertexMutator(graph, datastore)
    val vertexFetcher = new DatastoreVertexFetcher(graph, datastore)

    val mutateFuture = vertexMutator.mutateVertex("zk", vertex, true)
    Await.result(mutateFuture, Duration("10 seconds"))

    val vqp = VertexQueryParam(Seq(vertexId))
    val fetchFuture = vertexFetcher.fetchVertices(vqp)
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

  }
}
