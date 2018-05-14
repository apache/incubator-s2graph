package org.apache.s2graph.s2jobs.task.custom.process

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.fetcher.annoy.AnnoyModelFetcher
import org.apache.s2graph.core.{Query, QueryParam, ResourceManager}
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.s2jobs.BaseSparkTest
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.task.custom.sink.AnnoyIndexBuildSink

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.io.Source

class ALSModelProcessTest extends BaseSparkTest {
  import scala.collection.JavaConverters._

  // this test require adding movie
  def annoyLabelOptions(indexPath: String): String = {
    val options = s"""{
                     | "fetcher": {
                     |   "${ResourceManager.ClassNameKey}": "org.apache.s2graph.core.fetcher.annoy.AnnoyModelFetcher",
                     |   "${AnnoyModelFetcher.IndexFilePathKey}": "${indexPath}",
                     |   "${AnnoyModelFetcher.DimensionKey}": 10
                     | }
                     |}""".stripMargin
    options
  }
  def createLabel(labelName: String): Label = {
    val management = s2.management
    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "movie", "string", Seq(Prop("age", "0", "int", true)))

    Label.findByName(labelName, useCache = false) match {
      case None =>
        management.createLabel(
          labelName,
          service.serviceName, serviceColumn.columnName, serviceColumn.columnType,
          service.serviceName, serviceColumn.columnName, serviceColumn.columnType,
          service.serviceName,
          Seq.empty[Index],
          Seq.empty[Prop],
          isDirected = true,
          consistencyLevel = "strong",
          hTableName = None,
          hTableTTL = None,
          schemaVersion = "v3",
          compressionAlgorithm = "gz",
          options = None
        ).get
      case Some(label) => label
    }
  }

  def registerEdgeFetcher(labelName: String, indexPath: String): Label = {
    val label = createLabel(labelName)
    s2.management.updateEdgeFetcher(label, Option(annoyLabelOptions(indexPath)))

    Thread.sleep(10000)

    label
  }

  def buildALS(ratingsPath: String, indexPath: String) = {
    import spark.sqlContext.implicits._

    FileUtils.deleteQuietly(new File(indexPath))

    val buffer = scala.collection.mutable.ListBuffer.empty[(Int, Int, Float)]

    val lines = Source.fromFile(ratingsPath).getLines()
    // skip over header.
    lines.next()

    while (lines.hasNext) {
      val line = lines.next()
      try {
        val Array(userId, movieId, rating, ts) = line.split(",")
        buffer += ((userId.toInt, movieId.toInt, rating.toFloat))
      } catch {
        case e: Exception => // skip over.
      }
    }

    val rating = buffer.toDF("userId", "movieId", "rating")

    val processConf = TaskConf(name = "test", `type` = "test", inputs = Nil,
      options = Map.empty)

    val process = new ALSModelProcess(processConf)
    val df = process.execute(spark, Map("test" -> rating))

    val sinkConf = TaskConf(name = "sink", `type` = "sink", inputs = Nil,
      options = Map("path" -> indexPath, "itemFactors" -> indexPath))

    val sink = new AnnoyIndexBuildSink("sinkTest", sinkConf)
    sink.write(df)
  }

  def generateDataset = {
    import sys.process._

    val generateInputScript = "sh ./example/movielens/generate_input.sh"

    this.synchronized {
      generateInputScript !
    }
  }

  //TODO: make this test case to run smoothly
  ignore("ALS ModelProcess and AnnoyIndexBuildSink") {
    val labelName = "annoy_index_test"

    generateDataset

//    val inputPath = getClass.getResource("/ratings.csv").toURI.toString
    val inputPath = "input/ratings.csv"
    val indexPath = "annoy_result"
//    val dictPath = "input/movie.dict"

    buildALS(inputPath, indexPath)


    val label = registerEdgeFetcher(labelName, indexPath)

    val service = s2.management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      s2.management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))

    val vertex = s2.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "1")
    val queryParam = QueryParam(labelName = labelName, limit = 5)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val stepResult = Await.result(s2.getEdges(query), Duration("60 seconds"))

    stepResult.edgeWithScores.foreach { es =>
      println(es.edge.tgtVertex.innerIdVal)
    }

    Label.delete(label.id.get)
    FileUtils.deleteDirectory(new File("input"))
    FileUtils.deleteDirectory(new File("annoy_result"))
  }
}
