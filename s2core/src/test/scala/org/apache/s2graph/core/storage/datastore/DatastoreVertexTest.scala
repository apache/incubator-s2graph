package org.apache.s2graph.core.storage.datastore

import com.google.appengine.api.datastore.{AsyncDatastoreService, DatastoreService, DatastoreServiceFactory}
import com.google.appengine.tools.development.testing.{LocalDatastoreServiceTestConfig, LocalServiceTestHelper}
import org.apache.s2graph.core.VertexQueryParam
import org.apache.s2graph.core.fetcher.BaseFetcherTest
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

class DatastoreVertexTest extends BaseFetcherTest {
  implicit val ec = ExecutionContext.global
  val helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig())
  val serviceName = "test"
  val columnName = "user"
  val labelName = "test_label"

  override def beforeAll: Unit = {
    super.beforeAll
    helper.setUp()
    initEdgeFetcher(serviceName, columnName, labelName, None)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    helper.tearDown()
  }

  test("test using test util.") {
    val dsService = DatastoreServiceFactory.getDatastoreService()
    val builder = graph.elementBuilder
    val vertexId = builder.newVertexId(serviceName)(columnName)("a")
    val vertex = builder.newVertex(vertexId)
    vertex.propertyInner(Cardinality.single, "name", "xxx")

    val vertexMutator = new DatastoreVertexMutator(dsService)
    val vertexFetcher = new DatastoreVertexFetcher(graph, dsService)

    val mutateFuture = vertexMutator.mutateVertex("zk", vertex, true)
    Await.result(mutateFuture, Duration("10 seconds"))

    val vqp = VertexQueryParam(Seq(vertexId))
    val fetchFuture = vertexFetcher.fetchVertices(vqp)
    val fetchedVertices = Await.result(fetchFuture, Duration("10 seconds"))
    fetchedVertices.foreach { v =>
      println(v)
    }
  }
}
