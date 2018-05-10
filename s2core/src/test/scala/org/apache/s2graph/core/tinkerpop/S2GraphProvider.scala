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

import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.S2Graph.{DefaultColumnName, DefaultServiceName}
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.types.{HBaseType, InnerVal, VertexId}
import org.apache.s2graph.core.utils.logger
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.{Element, Graph, T}
import org.apache.tinkerpop.gremlin.{AbstractGraphProvider, LoadGraphWith}
import java.util

import scala.collection.JavaConverters._

object S2GraphProvider {

  val Implementation: Set[Class[_]] = Set(
    classOf[S2EdgeLike],
    classOf[S2Edge],
    classOf[S2Vertex],
    classOf[S2VertexLike],
    classOf[S2Property[_]],
    classOf[S2VertexProperty[_]],
    classOf[S2Graph]
  )
}

class S2GraphProvider extends AbstractGraphProvider {

  override def getBaseConfiguration(s: String, aClass: Class[_], s1: String, graphData: GraphData): util.Map[String, AnyRef] = {
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
        S2GraphFactory.cleanupDefaultSchema
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
    val mnt = s2Graph.management

    S2GraphFactory.cleanupDefaultSchema
    initTestSchema(testClass, testName)
    S2GraphFactory.initDefaultSchema(s2Graph)

    val defaultService = Service.findByName(S2Graph.DefaultServiceName).getOrElse(throw new IllegalStateException("default service is not initialized."))
    val defaultServiceColumn = ServiceColumn.find(defaultService.id.get, S2Graph.DefaultColumnName).getOrElse(throw new IllegalStateException("default column is not initialized."))

    val allProps = scala.collection.mutable.Set.empty[Prop]

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
    allProps ++= knowsProp

    // Change dataType for ColumnMeta('aKey') for PropertyFeatureSupportTest
    if (testClass.getSimpleName == "PropertyFeatureSupportTest") {
      knowsProp = knowsProp.filterNot(_.name == "aKey")

      val (dataType, defaultValue) = if (testName.toLowerCase.contains("boolean")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "false", "boolean")
        ("boolean", "false")
      } else if (testName.toLowerCase.contains("integer")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0", "integer")
        ("integer", "0")
      } else if (testName.toLowerCase.contains("long")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0", "long")
        ("long", "0")
      } else if (testName.toLowerCase.contains("double")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0.0", "double")
        ("double", "0.0")
      } else if (testName.toLowerCase.contains("float")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0.0", "float")
        ("float", "0.0")
      } else if (testName.toLowerCase.contains("string")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "-", "string")
        ("string", "-")
      } else {
        ("string", "-")
      }

      ColumnMeta.findByName(defaultServiceColumn.id.get, "aKey", useCache = false).foreach(cm => ColumnMeta.delete(cm.id.get))
      ColumnMeta.findOrInsert(defaultServiceColumn.id.get, "aKey", dataType, defaultValue, storeInGlobalIndex = true, useCache = false)
    }
    if (loadGraphWith != null && loadGraphWith.value() == GraphData.MODERN) {
      mnt.createLabel("knows", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
        defaultService.serviceName, Nil, knowsProp, true, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    } else {
      mnt.createLabel("knows", defaultService.serviceName, "vertex", "integer", defaultService.serviceName, "vertex", "integer",
        defaultService.serviceName, Nil, knowsProp, true, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    }

    // columns
    val personProps = Seq(Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string"))
    allProps ++= personProps
    val personColumn = Management.createServiceColumn(defaultService.serviceName, "person", "integer", personProps)

    val softwareProps = Seq(Prop("name", "-", "string"), Prop("lang", "-", "string"), Prop("temp", "-", "string"))
    allProps ++= softwareProps
    val softwareColumn = Management.createServiceColumn(defaultService.serviceName, "software", "integer", softwareProps)

    val productProps = Nil
    val productColumn = Management.createServiceColumn(defaultService.serviceName, "product", "integer", productProps)

    val dogProps = Nil
    val dogColumn = Management.createServiceColumn(defaultService.serviceName, "dog", "integer", dogProps)

    val animalProps = Seq(Prop("age", "0", "integer"), Prop("name", "-", "string"))
    allProps ++= animalProps
    val animalColumn = Management.createServiceColumn(defaultService.serviceName, "animal", "integer", animalProps)

    val songProps = Seq(Prop("name", "-", "string"), Prop("songType", "-", "string"), Prop("performances", "0", "integer"))
    allProps ++= songProps
    val songColumn = Management.createServiceColumn(defaultService.serviceName, "song", "integer", songProps)

    val artistProps = Seq(Prop("name", "-", "string"))
    allProps ++= artistProps
    val artistColumn = Management.createServiceColumn(defaultService.serviceName, "artist", "integer", artistProps)


    val stephenProps = Seq(Prop("name", "-", "string"))
    allProps ++= stephenProps
    val stephenColumn = Management.createServiceColumn(defaultService.serviceName, "STEPHEN", "integer", stephenProps)

    // labels
    val createdProps = Seq(Prop("weight", "0.0", "double"), Prop("name", "-", "string"))
    allProps ++= createdProps
    val created =
      if (loadGraphWith != null && loadGraphWith.value() == GraphData.MODERN) {
        mnt.createLabel("created",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "software", "integer",
          defaultService.serviceName, Nil, createdProps, true, "strong", None, None)
      } else {
        mnt.createLabel("created",
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, Nil, createdProps, true, "strong", None, None)
      }

    val boughtProps = Seq(Prop("x", "-", "string"), Prop("y", "-", "string"))
    allProps ++= boughtProps
    val bought = mnt.createLabel("bought", defaultService.serviceName, "person", "integer", defaultService.serviceName, "product", "integer",
      defaultService.serviceName, Nil, boughtProps, true, "strong", None, None,
      options = Option("""{"skipReverse": true}"""))

    val testProps = Seq(Prop("xxx", "-", "string"))
    allProps ++= testProps
    val test = mnt.createLabel("test", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, testProps, true, "weak", None, None,
      options = Option("""{"skipReverse": true}"""))

    val selfProps = Seq(Prop("__id", "-", "string"),  Prop("acl", "-", "string"),
      Prop("test", "-", "string"), Prop("name", "-", "string"), Prop("some", "-", "string"))
    allProps ++= selfProps

    val self =
      if (loadGraphWith != null && loadGraphWith.value() == GraphData.CLASSIC) {
        mnt.createLabel("self",
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, Nil, selfProps, true, "strong", None, None,
          options = Option("""{"skipReverse": true}"""))
      } else {
        mnt.createLabel("self", defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, Nil, selfProps, true, "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      }

    val friendsProps = Seq(Prop("weight", "0.0", "double"))
    allProps ++= friendsProps

    val friends =
      if (testClass.getSimpleName == "IoVertexTest") {
        mnt.createLabel("friends",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, Nil, friendsProps, true,
          "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      } else {
        mnt.createLabel("friends", defaultService.serviceName, defaultServiceColumn.columnName,
          defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, Nil, Nil, true,
          "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      }

    val friendProps = Seq(
      Prop("name", "-", "string"),
      Prop("location", "-", "string"),
      Prop("status", "-", "string"),
      Prop("weight", "0.0", "double"),
      Prop("acl", "-", "string")
    )
    allProps ++= friendProps

    val friend =
      if (testClass.getSimpleName == "IoEdgeTest") {
        mnt.createLabel("friend",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, "person", "integer",
          defaultService.serviceName, Nil, friendProps, true, "strong", None, None,
          options = Option("""{"skipReverse": false}"""))
      } else {
        mnt.createLabel("friend",
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
          defaultService.serviceName, Nil, friendProps, true, "strong", None, None,
          options = Option("""{"skipReverse": false}""")
        )
      }

    val hateProps = Nil
    val hate = mnt.createLabel("hate", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, hateProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val collaboratorProps = Seq(Prop("location", "-", "string"))
    allProps ++= collaboratorProps

    val collaborator = mnt.createLabel("collaborator", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, collaboratorProps, true, "strong", None, None,
       options = Option("""{"skipReverse": true}""")
    )

    val test1Props = Nil
    val test1 = mnt.createLabel("test1", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, test1Props, true, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val test2Props = Nil
    val test2 = mnt.createLabel("test2", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, test2Props, true, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val test3Props = Nil
    val test3 = mnt.createLabel("test3", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, test3Props, true, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val petsProps = Nil
    val pets = mnt.createLabel("pets", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, petsProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val walksProps = Seq(Prop("location", "-", "string"))
    allProps ++= walksProps

    val walks = mnt.createLabel("walks", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, walksProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val livesWithProps = Nil
    val livesWith = mnt.createLabel("livesWith", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, livesWithProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val hatesProps = Nil
    val hates = mnt.createLabel("hates", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, hatesProps, true, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val linkProps = Nil
    val link = mnt.createLabel("link", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, linkProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val codeveloperProps = Seq( Prop("year", "0", "integer"))
    allProps ++= codeveloperProps

    val codeveloper = mnt.createLabel("codeveloper",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, Nil, codeveloperProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val createdByProps = Seq(
      Prop("weight", "0.0", "double"),
      Prop("year", "0", "integer"),
      Prop("acl", "-", "string")
    )
    allProps ++= createdByProps

    val createdBy = mnt.createLabel("createdBy",
      defaultService.serviceName, "software", "integer",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, Nil, createdByProps, true, "strong", None, None)

    val existsWithProps = Seq(Prop("time", "-", "string"))
    allProps ++= existsWithProps

    val existsWith = mnt.createLabel("existsWith",
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      defaultService.serviceName, Nil, existsWithProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val followedByProps = Seq(Prop("weight", "0.0", "double"))
    allProps ++= followedByProps

    val followedBy = mnt.createLabel("followedBy",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, Nil, followedByProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val writtenByProps = Seq(Prop("weight", "0.0", "double"))
    allProps ++= writtenByProps

    val writtenBy = mnt.createLabel("writtenBy",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, "artist", "integer",
      defaultService.serviceName, Nil, writtenByProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val sungByProps = Seq(Prop("weight", "0.0", "double"))
    allProps ++= sungByProps

    val sungBy = mnt.createLabel("sungBy",
      defaultService.serviceName, "song", "integer",
      defaultService.serviceName, "artist", "integer",
      defaultService.serviceName, Nil, sungByProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val usesProps = Nil
    val uses = mnt.createLabel("uses",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "software", "integer",
      defaultService.serviceName, Nil, usesProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val likesProps = Seq(Prop("year", "0", "integer"))
    allProps ++= likesProps

    val likes = mnt.createLabel("likes",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, Nil, likesProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val fooProps = Seq(Prop("year", "0", "integer"))
    allProps ++= fooProps

    val foo = mnt.createLabel("foo",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, Nil, fooProps, true, "strong", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val barProps = Seq(Prop("year", "0", "integer"))
    allProps ++= barProps

    val bar = mnt.createLabel("bar",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, "person", "integer",
      defaultService.serviceName, Nil, barProps, true, "strong", None, None,
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
