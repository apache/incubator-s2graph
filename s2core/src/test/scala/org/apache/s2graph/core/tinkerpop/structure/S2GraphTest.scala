/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.tinkerpop.structure

import java.util.function.Predicate

import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{GlobalIndex, Service, ServiceColumn}
import org.apache.s2graph.core.tinkerpop.S2GraphProvider
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.process.traversal.{Compare, P, Scope}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.{in, out}
import org.apache.tinkerpop.gremlin.structure._
import org.scalatest.{FunSuite, Matchers}


class S2GraphTest extends FunSuite with Matchers with TestCommonWithModels {

  import scala.concurrent.ExecutionContext.Implicits.global

  initTests()

  val g = new S2Graph(config)
  lazy val gIndex = management.buildGlobalIndex(GlobalIndex.EdgeType, "S2GraphTest2", Seq("weight"))
  def printEdges(edges: Seq[Edge]): Unit = {
    edges.foreach { edge =>
      logger.debug(s"[FetchedEdge]: $edge")
    }
  }

  import scala.language.implicitConversions

//  def newVertexId(id: Any, label: Label = labelV2) = g.newVertexId(label.srcService, label.srcColumn, id)
//
//  def addVertex(id: AnyRef, label: Label = labelV2) =
//    g.addVertex(T.label, label.srcService.serviceName + S2Vertex.VertexLabelDelimiter + label.srcColumnName,
//      T.id, id).asInstanceOf[S2Vertex]
//
//  val srcId = Long.box(20)
//  val range = (100 until 110)
//  testData(srcId, range)

  //  val testProps = Seq(
  //    Prop("affinity_score", "0.0", DOUBLE),
  //    Prop("is_blocked", "false", BOOLEAN),
  //    Prop("time", "0", INT),
  //    Prop("weight", "0", INT),
  //    Prop("is_hidden", "true", BOOLEAN),
  //    Prop("phone_number", "xxx-xxx-xxxx", STRING),
  //    Prop("score", "0.1", FLOAT),
  //    Prop("age", "10", INT)
  //  )
//  def testData(srcId: AnyRef, range: Range, label: Label = labelV2) = {
//    val src = addVertex(srcId)
//
//    for {
//      i <- range
//    } {
//      val tgt = addVertex(Int.box(i))
//
//      src.addEdge(labelV2.label, tgt,
//        "age", Int.box(10),
//        "affinity_score", Double.box(0.1),
//        "is_blocked", Boolean.box(true),
//        "ts", Long.box(i))
//    }
//  }

//  test("test traversal.") {
//    val vertices = g.traversal().V(newVertexId(srcId)).out(labelV2.label).toSeq
//
//    vertices.size should be(range.size)
//    range.reverse.zip(vertices).foreach { case (tgtId, vertex) =>
//      val vertexId = g.newVertexId(labelV2.tgtService, labelV2.tgtColumn, tgtId)
//      val expectedId = g.newVertex(vertexId)
//      vertex.asInstanceOf[S2Vertex].innerId should be(expectedId.innerId)
//    }
//  }
//
//  test("test traversal. limit 1") {
//    val vertexIdParams = Seq(newVertexId(srcId))
//    val t: GraphTraversal[Vertex, Double] = g.traversal().V(vertexIdParams: _*).outE(labelV2.label).limit(1).values("affinity_score")
//    for {
//      affinityScore <- t
//    } {
//      logger.debug(s"$affinityScore")
//      affinityScore should be (0.1)
//    }
//  }
//  test("test traversal. 3") {
//
//    val l = label
//
//    val srcA = addVertex(Long.box(1), l)
//    val srcB = addVertex(Long.box(2), l)
//    val srcC = addVertex(Long.box(3), l)
//
//    val tgtA = addVertex(Long.box(101), l)
//    val tgtC = addVertex(Long.box(103), l)
//
//    srcA.addEdge(l.label, tgtA)
//    srcA.addEdge(l.label, tgtC)
//    tgtC.addEdge(l.label, srcB)
//    tgtA.addEdge(l.label, srcC)
//
//    val vertexIdParams = Seq(srcA.id)
//    val vertices = g.traversal().V(vertexIdParams: _*).out(l.label).out(l.label).toSeq
//    vertices.size should be(2)
//    vertices.foreach { v =>
//      val vertex = v.asInstanceOf[S2Vertex]
//      // TODO: we have too many id. this is ugly and confusing so fix me.
//      vertex.id.innerId == srcB.id.innerId || vertex.id.innerId == srcC.id.innerId should be(true)
//      logger.debug(s"[Vertex]: $v")
//    }
//  }
//  test("add vertex without params.") {
//    val vertex = g.addVertex().asInstanceOf[S2Vertex]
//    vertex.id.column.service.serviceName should be(g.DefaultService.serviceName)
//    vertex.id.column.columnName should be(g.DefaultColumn.columnName)
//  }
//  val s2Graph = graph.asInstanceOf[S2Graph]
//  val mnt = s2Graph.getManagement()
//  override val service = s2Graph.DefaultService
//
//  val personColumn = Management.createServiceColumn(service.serviceName, "person", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer")))
//  val softwareColumn = Management.createServiceColumn(service.serviceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
//  //    val vertexColumn = Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))
//
//  val created = mnt.createLabel("created", service.serviceName, "person", "integer", service.serviceName, "software", "integer",
//    true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "float")), "strong", None, None)
//
//  val knows = mnt.createLabel("knows", service.serviceName, "person", "integer", service.serviceName, "person", "integer",
//    true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "float")), "strong", None, None)
//

//  test("tinkerpop class graph test.") {
//    val marko = graph.addVertex(T.label, "person", T.id, Int.box(1))
//    marko.property("name", "marko")
//    marko.property("age", Int.box(29))
//    val vadas = graph.addVertex(T.label, "person", T.id, Int.box(2))
//    vadas.property("name", "vadas", "age", Int.box(27))
//    val lop = graph.addVertex(T.label, "software", T.id, Int.box(3), "name", "lop", "lang", "java")
//    val josh = graph.addVertex(T.label, "person", T.id, Int.box(4), "name", "josh", "age", Int.box(32))
//    val ripple = graph.addVertex(T.label, "software", T.id, Int.box(5), "name", "ripple", "lang", "java")
//    val peter = graph.addVertex(T.label, "person", T.id, Int.box(6), "name", "peter", "age", Int.box(35))
//
//    marko.addEdge("knows", vadas, T.id, Int.box(7), "weight", Float.box(0.5f))
//    marko.addEdge("knows", josh, T.id, Int.box(8), "weight", Float.box(1.0f))
//    marko.addEdge("created", lop, T.id, Int.box(9), "weight", Float.box(0.4f))
//    josh.addEdge("created", ripple, T.id, Int.box(10), "weight", Float.box(1.0f))
//    josh.addEdge("created", lop, T.id, Int.box(11), "weight", Float.box(0.4f))
//    peter.addEdge("created", lop, T.id, Int.box(12), "weight", Float.box(0.2f))
//    graph.tx().commit()
//
//    graph.traversal().V().inV()
//    val verticees = graph.traversal().V().asAdmin()
//
//
//    val vs = verticees.toList
//
//
//  }

//  test("addVertex with empty parameter") {
//
//  }
//  test("aaa") {
//    val mnt = graph.management
//    val defaultService = graph.DefaultService
//    val defaultServiceColumn = graph.DefaultColumn
//    val columnNames = Set(defaultServiceColumn.columnName, "person", "software", "product", "dog")
//    val labelNames = Set("knows", "created", "bought", "test", "self", "friends", "friend", "hate", "collaborator", "test1", "test2", "test3", "pets", "walks")
//
//    Management.deleteService(defaultService.serviceName)
//    columnNames.foreach { columnName =>
//      Management.deleteColumn(defaultServiceColumn.service.serviceName, columnName)
//    }
//    labelNames.foreach { labelName =>
//      Management.deleteLabel(labelName)
//    }
//
//    val knows = mnt.createLabel("knows",
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      true, defaultService.serviceName, Nil, Seq(Prop("since", "0", "integer")), consistencyLevel = "strong", None, None,
//      options = Option("""{"skipReverse": false}"""))
//
//    val pets = mnt.createLabel("pets",
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      true, defaultService.serviceName, Nil, Nil, consistencyLevel = "strong", None, None,
//      options = Option("""{"skipReverse": false}"""))
//
//    val walks = mnt.createLabel("walks",
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      true, defaultService.serviceName, Nil, Seq(Prop("location", "-", "string")), consistencyLevel = "strong", None, None,
//      options = Option("""{"skipReverse": false}"""))
//
//    val livesWith = mnt.createLabel("livesWith",
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      true, defaultService.serviceName, Nil, Nil, consistencyLevel = "strong", None, None,
//      options = Option("""{"skipReverse": false}"""))
//
//    val friend = mnt.createLabel("friend", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
//      true, defaultService.serviceName, Nil,
//      Seq(
//        Prop("name", "-", "string"),
//        Prop("location", "-", "string"),
//        Prop("status", "-", "string")
//      ),
//      "strong", None, None,
//      options = Option("""{"skipReverse": false}""")
//    )
//
//    val v1 = graph.addVertex("name", "marko")
//    val v2 = graph.addVertex("name", "puppy")
//
//    v1.addEdge("knows", v2, "since", Int.box(2010))
//    v1.addEdge("pets", v2)
//    v1.addEdge("walks", v2, "location", "arroyo")
//    v2.addEdge("knows", v1, "since", Int.box(2010))
//
//    v1.edges(Direction.BOTH).foreach { e => logger.error(s"[Edge]: $e")}
//  }

//  test("bb") {
//    val mnt = graph.management
//    val defaultService = graph.DefaultService
//    val defaultServiceColumn = graph.DefaultColumn
//    val columnNames = Set(defaultServiceColumn.columnName, "person", "software", "product", "dog")
//    val labelNames = Set("knows", "created", "bought", "test", "self", "friends", "friend", "hate", "collaborator", "test1", "test2", "test3", "pets", "walks")
//
//    Management.deleteService(defaultService.serviceName)
//    columnNames.foreach { columnName =>
//      Management.deleteColumn(defaultServiceColumn.service.serviceName, columnName)
//    }
//    labelNames.foreach { labelName =>
//      Management.deleteLabel(labelName)
//    }
//    val personColumn = Management.createServiceColumn(defaultService.serviceName, "person", "integer",
//      Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))
//    val knows = mnt.createLabel("knows",
//      defaultService.serviceName, "person", "integer",
//      defaultService.serviceName, "person", "integer",
//      true, defaultService.serviceName, Nil, Seq(Prop("since", "0", "integer"), Prop("year", "0", "integer")), consistencyLevel = "strong", None, None)
//
//    val created = mnt.createLabel("created", defaultService.serviceName, "person", "integer", defaultService.serviceName, "software", "integer",
//      true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)
//
////    val v1 = graph.toVertex(graph.DefaultService.serviceName, "person", 1)
////    val v4 = graph.toVertex(graph.DefaultService.serviceName, "person", 4)
////    val ts = System.currentTimeMillis()
////    val edge = graph.newEdge(v1, v4, knows.get,
////      GraphUtil.directions("out"), GraphUtil.operations("insert"),
////      propsWithTs = Map(LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, knows.get.schemaVersion)))
//    val v1 = graph.addVertex(T.label, "person", T.id, Int.box(1), "name", "marko")
//    val v4 = graph.addVertex(T.label, "person", T.id, Int.box(4), "name", "vadas")
//
//    val g = graph.traversal()
//    v1.addEdge("knows", v4, "year", Int.box(2002))
//
//    def convertToEdgeId(outVertexName: String, edgeLabel: String, inVertexName: String): AnyRef = {
//      g.V().has("name", outVertexName).outE(edgeLabel).as("e").inV.has("name", inVertexName).select[Edge]("e").next().id()
//    }
//
//    g.V().has("name", "marko").outE("knows").as("e").inV.foreach(e => logger.error(s"[Edge]: $e"))
//
////      .as("e").inV.has("name", "vadas").select[Edge]("e").next().id()
////    g.E(convertToEdgeId("marko", "knows", "vadas")).foreach(e => logger.error(s"[EDGE]: $e"))
////    val x = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), true)
//////      .hashCode()
////    val y = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), true)
////      .hashCode()
////    logger.error(s"[X]: $x")
////    logger.error(s"[Y]: $y")
//
////    g.E().foreach(e => logger.error(s"[Edge]: $e"))
////    g.V().has("name", "marko").outE("knows").foreach(v => logger.error(s"[OutVertex]: $v"))
////    g.V().has("name", "vadas").inE("knows").foreach(v => logger.error(s"[InVertex]: $v"))
//
//
//    @Test
//    @LoadGraphWith(GraphData.MODERN)
//    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
//    def shouldConstructDetachedEdgeAsReference() {
//
//      graph.traversal().E(convertToEdgeId("marko", "knows", "vadas")).next().property("year", 2002);
//      val detachedEdge = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), false);
////      assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
////      assertEquals("knows", detachedEdge.label());
////      assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.OUT).next().getClass());
////      assertEquals(convertToVertexId("marko"), detachedEdge.vertices(Direction.OUT).next().id());
////      assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
////      assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.IN).next().getClass());
////      assertEquals(convertToVertexId("vadas"), detachedEdge.vertices(Direction.IN).next().id());
////      assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
////
////      assertEquals(0, IteratorUtils.count(detachedEdge.properties()));
//    }
////    shouldConstructDetachedEdgeAsReference()
//  }
  def convertToEdgeId(g: GraphTraversalSource, outVertexName: String, edgeLabel: String, inVertexName: String): AnyRef = {
    g.V().has("name", outVertexName).outE(edgeLabel).as("e").inV.has("name", inVertexName).select[Edge]("e").next().id()
  }
//  test("ccc") {
//    val mnt = graph.management
//    val defaultService = graph.DefaultService
//    val defaultServiceColumn = graph.DefaultColumn
//    val columnNames = Set(defaultServiceColumn.columnName, "person", "software", "product", "dog")
//    val labelNames = Set("knows", "created", "bought", "test", "self", "friends", "friend", "hate", "collaborator", "test1", "test2", "test3", "pets", "walks")
//
//    Management.deleteService(defaultService.serviceName)
//    columnNames.foreach { columnName =>
//      Management.deleteColumn(defaultServiceColumn.service.serviceName, columnName)
//    }
//    labelNames.foreach { labelName =>
//      Management.deleteLabel(labelName)
//    }
//    val personColumn = Management.createServiceColumn(defaultService.serviceName, "person", "integer",
//      Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))
//    val knows = mnt.createLabel("knows",
//      defaultService.serviceName, "person", "integer",
//      defaultService.serviceName, "person", "integer",
//      true, defaultService.serviceName, Nil, Seq(Prop("since", "0", "integer"), Prop("year", "0", "integer")), consistencyLevel = "strong", None, None)
//
//    val created = mnt.createLabel("created",
//      defaultService.serviceName, "person", "integer",
//      defaultService.serviceName, "person", "integer",
//      true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)
//
//    val g = graph.traversal()
//    val v1 = graph.addVertex(T.label, "person", T.id, Int.box(1), "name", "josh")
//    val v4 = graph.addVertex(T.label, "person", T.id, Int.box(4), "name", "lop")
//    val e = v1.addEdge("created", v4)
//
//    val toDetach = g.E(convertToEdgeId(g, "josh", "created", "lop")).next()
//    val outV = toDetach.vertices(Direction.OUT).next()
//    val detachedEdge = DetachedFactory.detach(toDetach, true)
//    val attached = detachedEdge.attach(Attachable.Method.get(outV))
//
//    assert(toDetach.equals(attached))
//    assert(!attached.isInstanceOf[DetachedEdge])
//  }

//  test("ddd") {
////    @Test
////    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
////    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
////    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = EdgeFeatures.FEATURE_ADD_PROPERTY)
////    def shouldEnableFeatureOnEdgeIfNotEnabled() = {
////      graph.features.supports(classOf[EdgePropertyFeatures], "BooleanValues")
//////      assumeThat(graph.features().supports(EdgePropertyFeatures.class, featureName), is(false));
//////      try {
//////        final Edge edge = createEdgeForPropertyFeatureTests();
//////        edge.property("aKey", value);
//////        fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgePropertyFeatures.class.getSimpleName(), featureName));
//////      } catch (Exception e) {
//////        validateException(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value), e);
//////      }
////    }
//    val ret = graph.features.supports(classOf[EdgePropertyFeatures], "BooleanValues")
//    logger.error(s"[Support]: $ret")
////
////    @Test
////    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
////    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
////    public void shouldEnableFeatureOnVertexIfNotEnabled() throws Exception {
////      assumeThat(graph.features().supports(VertexPropertyFeatures.class, featureName), is(false));
////      try {
////        graph.addVertex("aKey", value);
////        fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), featureName));
////      } catch (Exception e) {
////        validateException(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value), e);
////      }
////    }
////
////
////
////    @Test
////    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
////    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
////    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_ANY_IDS, supported = false)
////    public void shouldSupportUserSuppliedIdsOfTypeAny() throws Exception {
////      try {
////        final Date id = new Date();
////        graph.addVertex(T.id, id);
////
////        // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
////        // to throw the excepStion
////        if (!graph.features().vertex().willAllowId(id))
////          fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_ANY_IDS));
////      } catch (Exception e) {
////        validateException(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
////      }
////    }
//  }
  ignore("Modern") {
    gIndex
    val mnt = graph.management

    S2GraphFactory.cleanupDefaultSchema
    S2GraphFactory.initDefaultSchema(graph)

    val softwareColumn = Management.createServiceColumn(S2Graph.DefaultServiceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
    val personColumn = Management.createServiceColumn(S2Graph.DefaultServiceName, "person", "integer",
      Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))

    val knows = mnt.createLabel("knows",
      S2Graph.DefaultServiceName, "person", "integer",
      S2Graph.DefaultServiceName, "person", "integer",
      true, S2Graph.DefaultServiceName, Nil, Seq(Prop("weight", "0.0", "double"), Prop("year", "0", "integer")), consistencyLevel = "strong", None, None)

    val created = mnt.createLabel("created",
      S2Graph.DefaultServiceName, "person", "integer",
      S2Graph.DefaultServiceName, "person", "integer",
      true, S2Graph.DefaultServiceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)

    val g = graph.traversal()
    val v1 = graph.addVertex(T.label, "person", T.id, Int.box(1), "name", "marko", "age", Int.box(29))
    val v2 = graph.addVertex(T.label, "person", T.id, Int.box(2), "name", "vadas", "age", Int.box(27))
    val v3 = graph.addVertex(T.label, "software", T.id, Int.box(3), "name", "lop", "lang", "java")
    val v4 = graph.addVertex(T.label, "person", T.id, Int.box(4), "name", "josh", "josh", Int.box(32))
    val v5 = graph.addVertex(T.label, "software", T.id, Int.box(5), "name", "ripple", "lang", "java")
    val v6 = graph.addVertex(T.label, "person", T.id, Int.box(6), "name", "peter", "age", Int.box(35))

    val e1 = v1.addEdge("created", v3, "weight", Double.box(0.4))

    val e2 = v1.addEdge("knows", v2, "weight", Double.box(0.5))
    val e3 = v1.addEdge("knows", v4, "weight", Double.box(1.0))


    val e4 = v2.addEdge("knows", v1, "weight", Double.box(0.5))

    val e5 = v3.addEdge("created", v1, "weight", Double.box(0.4))
    val e6 = v3.addEdge("created", v4, "weight", Double.box(0.4))
    val e7 = v3.addEdge("created", v6, "weight", Double.box(0.2))

    val e8 = v4.addEdge("knows", v1, "weight", Double.box(1.0))
    val e9 = v4.addEdge("created", v5, "weight", Double.box(1.0))
    val e10 = v4.addEdge("created", v3, "weight", Double.box(0.4))

    val e11 = v5.addEdge("created", v4, "weight", Double.box(1.0))

    val e12 = v6.addEdge("created", v3, "weight", Double.box(0.2))

    val ls = graph.traversal().E().has("knows", "weight", P.eq(Double.box(0.5)))

//    return graph.traversal.V().hasLabel("person").has("age", P.not(P.lte(10).and(P.not(P.between(11, 20)))).and(P.lt(29).or(P.eq(35)))).values("name")
    val l = ls.toList
    println(s"[Size]: ${l.size}")
    println(l.toArray.toSeq.mkString("\n"))

    ls
  }
}