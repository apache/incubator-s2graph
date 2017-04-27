package org.apache.s2graph.core.tinkerpop

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicLong

import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{ColumnMeta, Label, ServiceColumn}
import org.apache.s2graph.core.types.{HBaseType, InnerVal, VertexId}
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.{Element, Graph, T}
import org.apache.tinkerpop.gremlin.{AbstractGraphProvider, LoadGraphWith}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.s2graph.core.utils.logger

object S2GraphProvider {
  val Implementation: Set[Class[_]] = Set(
    classOf[S2Edge],
    classOf[S2Vertex],
    classOf[S2Property[_]],
    classOf[S2VertexProperty[_]],
    classOf[S2Graph]
  )
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
        cleanupSchema(graph)
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

  private def cleanupSchema(graph: Graph): Unit = {
    val s2Graph = graph.asInstanceOf[S2Graph]
    val mnt = s2Graph.getManagement()
    val defaultService = s2Graph.DefaultService
    val defaultServiceColumn = s2Graph.DefaultColumn

    val columnNames = Set(defaultServiceColumn.columnName, "person", "software", "product", "dog")
    val labelNames = Set("knows", "created", "bought", "test", "self", "friends", "friend", "hate", "collaborator", "test1", "test2", "test3", "pets", "walks", "hates", "link")

    Management.deleteService(defaultService.serviceName)
    columnNames.foreach { columnName =>
      Management.deleteColumn(defaultServiceColumn.service.serviceName, columnName)
    }
    labelNames.foreach { labelName =>
      Management.deleteLabel(labelName)
    }
  }

  override def loadGraphData(graph: Graph, loadGraphWith: LoadGraphWith, testClass: Class[_], testName: String): Unit = {
    val s2Graph = graph.asInstanceOf[S2Graph]
    val mnt = s2Graph.getManagement()
    val defaultService = s2Graph.DefaultService
    val defaultServiceColumn = s2Graph.DefaultColumn

    initTestSchema(testClass, testName)

    Management.deleteLabel("knows")

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
      Prop("data", "-", "string")
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

    if (testClass.getName.contains("SerializationTest") || testClass.getSimpleName == "IoPropertyTest") {
      mnt.createLabel("knows", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    } else if (testClass.getSimpleName.contains("CommunityGeneratorTest")) {
      mnt.createLabel("knows", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": true}"""))
    } else if (testClass.getSimpleName == "DetachedEdgeTest" ||
        testClass.getSimpleName.contains("GraphSONTest")) {
      mnt.createLabel("knows", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    } else {
      mnt.createLabel("knows", defaultService.serviceName, "vertex", "string", defaultService.serviceName, "vertex", "string",
        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
    }


//    if (testClass.getSimpleName.contains("VertexTest") || (testClass.getSimpleName == "EdgeTest" && testName == "shouldAutotypeDoubleProperties")) {
//      mnt.createLabel("knows", defaultService.serviceName, "vertex", "string", defaultService.serviceName, "vertex", "string",
//        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
//    } else {
//      mnt.createLabel("knows", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
//        true, defaultService.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": false}"""))
//    }

    val personColumn = Management.createServiceColumn(defaultService.serviceName, "person", "integer",
      Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))
    val softwareColumn = Management.createServiceColumn(defaultService.serviceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
    val productColumn = Management.createServiceColumn(defaultService.serviceName, "product", "integer", Nil)
    val dogColumn = Management.createServiceColumn(defaultService.serviceName, "dog", "integer", Nil)
//    val vertexColumn = Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))

    val created = mnt.createLabel("created", defaultService.serviceName, "person", "integer", defaultService.serviceName, "software", "integer",
      true, defaultService.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)

    val bought = mnt.createLabel("bought", defaultService.serviceName, "person", "integer", defaultService.serviceName, "product", "integer",
      true, defaultService.serviceName, Nil, Seq(Prop("x", "-", "string"), Prop("y", "-", "string")), "strong", None, None,
      options = Option("""{"skipReverse": true}"""))

    val test = mnt.createLabel("test", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Seq(Prop("xxx", "-", "string")), "weak", None, None,
      options = Option("""{"skipReverse": true}"""))

    val self = if (testClass.getSimpleName == "StarGraphTest") {
      mnt.createLabel("self", defaultService.serviceName, "person", "integer",
        defaultService.serviceName, "person", "integer",
        true, defaultService.serviceName, Nil, Seq(Prop("acl", "-", "string")), "strong", None, None,
        options = Option("""{"skipReverse": false}"""))
    } else if (testClass.getSimpleName.contains("GraphTest")) {
      mnt.createLabel("self", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
        true, defaultService.serviceName, Nil, Seq(Prop("acl", "-", "string")), "strong", None, None,
        options = Option("""{"skipReverse": true}"""))
    } else {
      mnt.createLabel("self", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
        true, defaultService.serviceName, Nil, Seq(Prop("acl", "-", "string")), "weak", None, None,
        options = Option("""{"skipReverse": true}"""))
    }

    val friends = mnt.createLabel("friends", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
      true, defaultService.serviceName, Nil, Nil,
      "strong", None, None,
      options = Option("""{"skipReverse": false}"""))

    val friend = if (testClass.getSimpleName.contains("IoEdgeTest")) {
      mnt.createLabel("friend", defaultService.serviceName, "person", "integer", defaultService.serviceName, "person", "integer",
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
      mnt.createLabel("friend", defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType, defaultService.serviceName, defaultServiceColumn.columnName, defaultServiceColumn.columnType,
        true, defaultService.serviceName, Nil,
        Seq(
          Prop("name", "-", "string"),
          Prop("location", "-", "string"),
          Prop("status", "-", "string"),
          Prop("weight", "0.0", "double"),
          Prop("acl", "-", "string")
        ),
        "strong", None, None,
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

    super.loadGraphData(graph, loadGraphWith, testClass, testName)
  }

  override def convertId(id: scala.Any, c: Class[_ <: Element]): AnyRef = {
    val isVertex = c.toString.contains("Vertex")
    if (isVertex) {
      VertexId(ServiceColumn.findAll().head, InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION))
    } else {
      EdgeId(
        InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION),
        InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION),
        "_s2graph",
        "out",
        System.currentTimeMillis()
      )
    }
  }

  override def convertLabel(label: String): String = {
    label
  }
}
