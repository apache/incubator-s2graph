package org.apache.s2graph.core.storage.datastore

import java.util.function.BiConsumer

import com.spotify.asyncdatastoreclient._
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import org.apache.s2graph.core.fetcher.BaseFetcherTest
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.{Management, QueryParam, QueryRequest, S2Graph, S2GraphConfigs, S2Vertex, S2VertexProperty, VertexQueryParam, Query => S2Query}
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.collection.JavaConverters._

class DatastoreStorageTest extends FunSuite with Matchers with BeforeAndAfterAll {
  import DatastoreStorage._
  implicit val ec = ExecutionContext.global
  var graph: S2Graph = _
  var parser: RequestParser = _
  var management: Management = _
  var config: Config = _

  val DATASTORE_HOST: String = System.getProperty("host", "http://localhost:8080")
  val PROJECT: String = System.getProperty("dataset", "async-test")
  val NAMESPACE: String = System.getProperty("namespace", "test")
  val KEY_PATH: String = System.getProperty("keypath")
  val VERSION: String = System.getProperty("version", "v1beta3")

  val serviceName = "test"
  val columnName = "user"
  val labelName = "test_label"

  var datastore: Datastore = _

  override def beforeAll = {

    config = ConfigFactory.load()
      .withValue(S2GraphConfigs.S2GRAPH_STORE_BACKEND, ConfigValueFactory.fromAnyRef("datastore"))

    graph = new S2Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)
    parser = new RequestParser(graph)

    datastore = DatastoreStorage.initDatastore(storageConfig)
    BaseFetcherTest.initEdgeFetcher(management, serviceName, columnName, labelName, None)
    removeAll()
  }

  override def afterAll(): Unit = {
    removeAll()
    graph.shutdown()
  }

  val storageConfig = ConfigFactory.parseMap(
    Map(
      HostKey -> DATASTORE_HOST,
      ProjectKey -> PROJECT,
      NamespaceKey -> NAMESPACE,
      KeyPathKey -> KEY_PATH,
      VersionKey -> VERSION
    ).asJava
  )

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
    Await.result(mutateFuture, Duration("60 seconds"))

    val vqp = VertexQueryParam(Seq(vertexId))
    val fetchFuture = fetcher.fetchVertices(vqp)
    val fetchedVertices = Await.result(fetchFuture, Duration("60 seconds"))
    fetchedVertices.foreach { v =>
      println(v)
      v.props.forEach(new BiConsumer[String, S2VertexProperty[_]] {
        override def accept(t: String, u: S2VertexProperty[_]): Unit = {
          println(s"key = ${t}, value = ${u.value}")
        }
      })

      vertex.id == v.id && vertex.props == v.props shouldBe true
    }
  }

  test("test edge.") {
    val builder = graph.elementBuilder
    val vertexId = builder.newVertexId(serviceName)(columnName)("user_1")
    val vertex = builder.newVertex(vertexId)

    val edge1 = builder.toEdge("user_1", "user_z", labelName, "out", Map("score" -> 0.1), ts = 10L, operation = "insert")
    val edge2 = builder.toEdge("user_1", "user_x", labelName, "out", Map("score" -> 0.8), ts = 9L, operation = "insert")

    val mutator = new DatastoreEdgeMutator(graph, datastore)
    val fetcher = new DatastoreEdgeFetcher(graph, datastore)

//    val mutateFuture = mutator.mutateWeakEdges("zk", Seq(edge1, edge2), true)
    val mutateFuture = mutator.mutateStrongEdges("zk", Seq(edge1, edge2), true)
    Await.result(mutateFuture, Duration("60 seconds"))

    val queryParam = QueryParam(labelName = labelName)

    val queryRequest = QueryRequest(S2Query.empty, 0, vertex, queryParam)
    val fetchFuture = fetcher.fetches(Seq(queryRequest), Map.empty)
    Await.result(fetchFuture, Duration("60 seconds")).foreach { stepResult =>
      val edges = stepResult.edgeWithScores.map(_.edge)
      edges.foreach(println)

      edges.size shouldBe 2

      edge1.edgeId == edges(0).edgeId && edge1.propsWithTs == edges(0).propsWithTs shouldBe true
      edge2.edgeId == edges(1).edgeId && edge2.propsWithTs == edges(1).propsWithTs shouldBe true
    }
  }
}
