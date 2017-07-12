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

package org.apache.s2graph.core.tinkerpop

import java.util

import com.typesafe.config.ConfigFactory
import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.S2Graph.{DefaultColumnName, DefaultServiceName}
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{ColumnMeta, Label, Service, ServiceColumn}
import org.apache.s2graph.core.types.{HBaseType, InnerVal, VertexId}
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.{Element, Graph, T}
import org.apache.tinkerpop.gremlin.{AbstractGraphProvider, LoadGraphWith}

import scala.collection.JavaConverters._

object S2GraphProvider {

  val Implementation: Set[Class[_]] = Set(
    classOf[S2Edge],
    classOf[S2Vertex],
    classOf[S2Property[_]],
    classOf[S2VertexProperty[_]],
    classOf[S2Graph]
  )
  def initDefaultSchema(graph: S2Graph): Unit = {
    val management = graph.management

    //    Management.deleteService(DefaultServiceName)
    val DefaultService = management.createService(DefaultServiceName, "localhost", "s2graph", 0, None).get

    //    Management.deleteColumn(DefaultServiceName, DefaultColumnName)
    val DefaultColumn = ServiceColumn.findOrInsert(DefaultService.id.get, DefaultColumnName, Some("integer"), HBaseType.DEFAULT_VERSION, useCache = false)

    val DefaultColumnMetas = {
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "test", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "name", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "age", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "lang", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "oid", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "communityIndex", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "testing", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "string", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "boolean", "boolean", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "long", "long", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "float", "float", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "double", "double", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "integer", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "aKey", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "x", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "y", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "location", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "status", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "myId", "integer", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "acl", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "some", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "this", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "that", "string", useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "any", "string", useCache = false)
    }

    //    Management.deleteLabel("_s2graph")
    val DefaultLabel = management.createLabel("_s2graph", DefaultService.serviceName, DefaultColumn.columnName, DefaultColumn.columnType,
      DefaultService.serviceName, DefaultColumn.columnName, DefaultColumn.columnType, true, DefaultService.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    ).get
  }

  def cleanupSchema: Unit = {
    val columnNames = Set(S2Graph.DefaultColumnName, "person", "software", "product", "dog",
      "animal", "song", "artist", "STEPHEN")

    val labelNames = Set(S2Graph.DefaultLabelName, "knows", "created", "bought", "test", "self", "friends", "friend", "hate", "collaborator",
      "test1", "test2", "test3", "pets", "walks", "hates", "link",
      "codeveloper", "createdBy", "existsWith", "writtenBy", "sungBy", "followedBy", "uses", "likes", "foo", "bar")

    columnNames.foreach { columnName =>
      Management.deleteColumn(S2Graph.DefaultServiceName, columnName)
    }
    labelNames.foreach { labelName =>
      Management.deleteLabel(labelName)
    }
  }
}

class S2GraphProvider extends AbstractGraphProvider {

  override def getBaseConfiguration(s: String, aClass: Class[_], s1: String, graphData: GraphData): util.Map[String, AnyRef] = {
    val config = ConfigFactory.load()
    val m = new java.util.HashMap[String, AnyRef]()
    m.put(Graph.GRAPH, classOf[S2Graph].getName)
//    m.put("db.default.url", "jdbc:h2:mem:db1;MODE=MYSQL")
    m
  }

  override def clear(graph: Graph, configuration: Configuration): Unit =
    if (graph != null) {
      val s2Graph = graph.asInstanceOf[S2Graph]
      if (s2Graph.isRunning) {
//        val labels = Label.findAll()
//        labels.groupBy(_.hbaseTableName).values.foreach { labelsWithSameTable =>
//          labelsWithSameTable.headOption.foreach { label =>
//            s2Graph.management.truncateStorage(label.label)
//          }
//        }
//        s2Graph.shutdown(modelDataDelete = true)
        S2GraphProvider.cleanupSchema
        s2Graph.shutdown(modelDataDelete = true)
        logger.info("S2Graph Shutdown")
      }
    }

  override def getImplementations: util.Set[Class[_]] = S2GraphProvider.Implementation.asJava

  def initTestSchema(testClass: Class[_], testName: String) = {
    val testClassName = testClass.getSimpleName
    testClass.getSimpleName match {
      case _ =>
    }
  }

//  override def openTestGraph(config: Configuration): Graph = new S2Graph(config)(ExecutionContext.global)

  override def loadGraphData(graph: Graph, loadGraphWith: LoadGraphWith, testClass: Class[_], testName: String): Unit = {
    val s2Graph = graph.asInstanceOf[S2Graph]
    val mnt = s2Graph.getManagement()

    S2GraphProvider.cleanupSchema
    initTestSchema(testClass, testName)
    S2GraphProvider.initDefaultSchema(s2Graph)

    val defaultService = Service.findByName(S2Graph.DefaultServiceName).getOrElse(throw new IllegalStateException("default service is not initialized."))
    val defaultServiceColumn = ServiceColumn.find(defaultService.id.get, S2Graph.DefaultColumnName).getOrElse(throw new IllegalStateException("default column is not initialized."))

    var knowsProp = Vector(
      Prop("weight", "0.0", "double"),
      Prop("data", "-", "string"),
      Prop("year", "0", "integer"),
      Prop("boolean", "false", "boolean"),
      Prop("float", "0.0", "float"),
      Prop("double", "0.0", "double"),
      Prop("long", "0.0", "long"),
      Prop("string", "-", "string"),
      Prop("integer", "0", "integer"),
      Prop("aKey", "-", "string"),
      Prop("stars", "0", "integer"),
      Prop("since", "0", "integer"),
      Prop("myEdgeId", "0", "integer"),
      Prop("data", "-", "string"),
      Prop("name", "-", "string")
    )

   // Change dataType for ColumnMeta('aKey') for PropertyFeatureSupportTest
    if (testClass.getSimpleName == "PropertyFeatureSupportTest") {
      knowsProp = knowsProp.filterNot(_.name == "aKey")

      val dataType = if (testName.toLowerCase.contains("boolean")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "false", "boolean")
        "boolean"
      } else if (testName.toLowerCase.contains("integer")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0", "integer")
        "integer"
      } else if (testName.toLowerCase.contains("long")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0", "long")
        "long"
      } else if (testName.toLowerCase.contains("double")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0.0", "double")
        "double"
      } else if (testName.toLowerCase.contains("float")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0.0", "float")
        "float"
      } else if (testName.toLowerCase.contains("string")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "-", "string")
        "string"
      } else {
        "string"
      }

      ColumnMeta.findByName(defaultServiceColumn.id.get, "aKey", useCache = false).foreach(cm => ColumnMeta.delete(cm.id.get))
      ColumnMeta.findOrInsert(defaultServiceColumn.id.get, "aKey", dataType, useCache = false)
    }
    if (loadGraphWith != null && loadGraphWith.value() == GraphData.MODERN) {
      mnt.createLabel("knows", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    } else {
      mnt.createLabel("knows", defaultService.serviceName, "vertex", "integer", defaultService.serviceName, "vertex", "integer",
        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    }

    // columns
    val personColumn = Management.createServiceColumn(defaultService.serviceName, "person", "integer",
      Seq(Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))
    val softwareColumn = Management.createServiceColumn(defaultService.serviceName, "software", "integer",
      Seq(Prop("name", "-", "string"), Prop("lang", "-", "string"), Prop("temp", "-", "string")))

    val productColumn = Management.createServiceColumn(defaultService.serviceName, "product", "integer", Nil)
    val dogColumn = Management.createServiceColumn(defaultService.serviceName, "dog", "integer", Nil)
    val animalColumn = Management.createServiceColumn(defaultService.serviceName, "animal", "integer", Seq(Prop("age", "0", "integer"), Prop("name", "-", "string")))
    val songColumn = Management.createServiceColumn(defaultService.serviceName, "song", "integer",
      Seq(Prop("name", "-", "string"), Prop("songType", "-", "string"), Prop("performances", "0", "integer"))
    )
    val artistColumn = Management.createServiceColumn(defaultService.serviceName, "artist", "integer",
      Seq(Prop("name", "-", "string"))
    )
    val stephenColumn = Management.createServiceColumn(defaultService.serviceName, "STEPHEN", "integer",
      Seq(Prop("name", "-", "string"))
    )
//    val vertexColumn = Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))

    // labels
    val createdProps = Seq(Prop("weight", "0.0", "double"), Prop("name", "-", "string"))

    val created =
      if (loadGraphWith != null && loadGraphWith.value() == GraphData.MODERN) {
        mnt.createLabel("created",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "software", "integer",
          true, defaultService.serviceName, Nil, createdProps, "strong", None, None)
      } else {
        mnt.createLabel("created",
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          true, defaultService.serviceName, Nil, createdProps, "strong", None, None)
      }


    val bought = mnt.createLabel("bought", defaultService.serviceName, "person", "integer", defaultService.serviceName, "product", "integer",
      true, defaultService.serviceName, Nil, Seq(Prop("x", "-", "string"), Prop("y", "-", "string")), "strong", None, None,
      options = Option("""{"skipReverse": true}"""))

    val test = mnt.createLabel("test", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Seq(Prop("xxx", "-", "string")), "weak", None, None,
      options = Option("""{"skipReverse": true}"""))

    val selfProps = Seq(Prop("__id", "-", "string"),  Prop("acl", "-", "string"),
      Prop("test", "-", "string"), Prop("name", "-", "string"), Prop("some", "-", "string"))

    val self =
      if (loadGraphWith != null && loadGraphWith.value() == GraphData.CLASSIC) {
        mnt.createLabel("self",
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          true, defaultService.serviceName, Nil, selfProps, "strong", None, None,
          options = Option("""{"skipReverse": true}"""))
      } else {
        mnt.createLabel("self", defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "person", "integer",
          true, defaultService.serviceName, Nil, selfProps, "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      }

    val friends =
      if (testClass.getSimpleName == "IoVertexTest") {
        mnt.createLabel("friends",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "person", "integer",
          true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")),
          "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      } else {
        mnt.createLabel("friends", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          true, defaultService.serviceName, Nil, Nil,
          "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      }

    val friend =
      if (testClass.getSimpleName == "IoEdgeTest") {
        mnt.createLabel("friend",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "person", "integer",
          true, defaultService.serviceName, Nil,
          Seq(
            Prop("name", "-", "string"),
            Prop("location", "-", "string"),
            Prop("status", "-", "string"),
            Prop("weight", "0.0", "double"),
            Prop("acl", "-", "string")
          ), "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      } else {
        mnt.createLabel("friend",
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          true, defaultService.serviceName, Nil,
          Seq(
            Prop("name", "-", "string"),
            Prop("location", "-", "string"),
            Prop("status", "-", "string"),
            Prop("weight", "0.0", "double"),
            Prop("acl", "-", "string")
          ), "strong", None, None,
          options = Option("""{"skipReverse": false}""")
        )
      }

    val hate = mnt.createLabel("hate", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val collaborator = mnt.createLabel("collaborator", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("location", "-", "string")
      ),
      "strong", None, None,
       options = Option("""{"skipReverse": true}""")
    )

    val test1 = mnt.createLabel("test1", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val test2 = mnt.createLabel("test2", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val test3 = mnt.createLabel("test3", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val pets = mnt.createLabel("pets", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val walks = mnt.createLabel("walks", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("location", "-", "string")
      ), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val livesWith = mnt.createLabel("livesWith", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val hates = mnt.createLabel("hates", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val link = mnt.createLabel("link", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val codeveloper = mnt.createLabel("codeveloper",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("year", "0", "integer")
      ), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val createdBy = mnt.createLabel("createdBy",
      defaultService.serviceName, "software", "integer",
      defaultService.serviceName, "person", "integer",
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("weight", "0.0", "double"),
        Prop("year", "0", "integer"),
        Prop("acl", "-", "string")
      ), "strong", None, None)

    val existsWith = mnt.createLabel("existsWith",
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Seq(Prop("time", "-", "string")), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val followedBy = mnt.createLabel("followedBy",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, "song", "integer",
      true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val writtenBy = mnt.createLabel("writtenBy",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, "artist", "integer",
      true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val sungBy = mnt.createLabel("sungBy",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, "artist", "integer",
      true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val uses = mnt.createLabel("uses",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "software", "integer",
      true, defaultService.serviceName, Nil, Nil, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val likes = mnt.createLabel("likes",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("year", "0", "integer")
      ), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val foo = mnt.createLabel("foo",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("year", "0", "integer")
      ), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val bar = mnt.createLabel("bar",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      true, defaultService.serviceName, Nil,
      Seq(
        Prop("year", "0", "integer")
      ), "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    super.loadGraphData(graph, loadGraphWith, testClass, testName)
  }

  override def convertId(id: scala.Any, c: Class[_ <: Element]): AnyRef = {
    val isVertex = c.toString.contains("Vertex")
    if (isVertex) {
      VertexId(ServiceColumn.findAll().head, InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION))
    } else {
      val label = Label.findByName("_s2graph").getOrElse(throw new LabelNotExistException("_s2graph"))
      EdgeId(
        VertexId(label.srcColumn, InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION)),
        VertexId(label.tgtColumn, InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION)),
        "_s2graph",
        "out",
        System.currentTimeMillis()
      )
    }
  }

  override def convertLabel(label: String): String = {
    label match {
      case "blah1" => S2Graph.DefaultLabelName
      case "blah2" => S2Graph.DefaultLabelName
      case _ => label
    }
  }
}
