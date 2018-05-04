package org.apache.s2graph.s2jobs.task.custom.process

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.{Query, QueryParam}
import org.apache.s2graph.core.model.{ANNIndexWithDict, AnnoyModelFetcher, HDFSImporter, ModelManager}
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.s2jobs.task.TaskConf

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.io.Source

class ALSModelProcessTest extends IntegrateCommon with DataFrameSuiteBase {
  import scala.collection.JavaConverters._

  // this test require adding movie lens rating data(u.data, movie.txt) under resources
  // so ignore for now until figure out how to automate download dataset.
//  ignore("RUN ALS on movie lens rating data and build annoy index on itemFeatures, finally query.") {
//    import spark.sqlContext.implicits._
//    val ratingPath = this.getClass.getResource("/u.data").toURI.getPath
//
//    val ratings = Source.fromFile(new File(ratingPath)).getLines().toSeq.map { line =>
//      val tokens = line.split("\t")
//      (tokens(0).toInt, tokens(1).toInt, tokens(2).toFloat)
//    }.toDF("userId", "movieId", "rating")
//
//    val outputPath = "/tmp"
//    val localInputPath = "/tmp/annoy_input"
//    val localIndexPath = "/tmp/annoy_result"
//
//    val taskOptions = Map(
//      "outputPath" -> outputPath,
//      "localInputPath" -> localInputPath,
//      "localIndexPath" -> localIndexPath
//    )
//
//    val conf = TaskConf("test", "test", Nil, taskOptions)
//    ALSModelProcess.buildAnnoyIndex(spark, conf, ratings)
//
//    val labelName = "annoy_model_fetcher_test"
//
//    val remoteIndexFilePath = s"${localIndexPath}/annoy-index"
//    val remoteDictFilePath = this.getClass.getResource(s"/movie.dict").toURI.getPath
//
//    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
//    val serviceColumn =
//      management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))
//
//    val options = s"""{
//                     | "importer": {
//                     |   "${ModelManager.ImporterClassNameKey}": "org.apache.s2graph.core.model.IdentityImporter"
//                     | },
//                     | "fetcher": {
//                     |   "${ModelManager.FetcherClassNameKey}": "org.apache.s2graph.core.model.AnnoyModelFetcher",
//                     |   "${AnnoyModelFetcher.IndexFilePathKey}": "${remoteIndexFilePath}",
//                     |   "${AnnoyModelFetcher.DictFilePathKey}": "${remoteDictFilePath}",
//                     |   "${AnnoyModelFetcher.DimensionKey}": 10
//                     | }
//                     |}""".stripMargin
//
//    Label.findByName(labelName, useCache = false).foreach { label => Label.delete(label.id.get) }
//
//    val label = management.createLabel(
//      labelName,
//      serviceColumn,
//      serviceColumn,
//      true,
//      service.serviceName,
//      Seq.empty[Index].asJava,
//      Seq.empty[Prop].asJava,
//      "strong",
//      null,
//      -1,
//      "v3",
//      "gz",
//      options
//    )
//    val config = ConfigFactory.parseString(options)
//    val importerFuture = graph.modelManager.importModel(label, config)(ExecutionContext.Implicits.global)
//    Await.result(importerFuture, Duration("3 minutes"))
//
//    Thread.sleep(10000)
//
//    val vertex = graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "Toy Story (1995)")
//    val queryParam = QueryParam(labelName = labelName, limit = 5)
//
//    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
//    val stepResult = Await.result(graph.getEdges(query), Duration("60 seconds"))
//
//    stepResult.edgeWithScores.foreach { es =>
//      println(es.edge.tgtVertex.innerIdVal)
//    }
//
//    // clean up temp directory.
//    FileUtils.deleteDirectory(new File(outputPath))
//  }

  def annoyLabelOptions(indexPath: String, dictPath: String): String = {
    val options = s"""{
                     | "importer": {
                     |   "${ModelManager.ImporterClassNameKey}": "org.apache.s2graph.core.model.IdentityImporter"
                     | },
                     | "fetcher": {
                     |   "${ModelManager.FetcherClassNameKey}": "org.apache.s2graph.core.model.AnnoyModelFetcher",
                     |   "${AnnoyModelFetcher.IndexFilePathKey}": "${indexPath}",
                     |   "${AnnoyModelFetcher.DictFilePathKey}": "${dictPath}",
                     |   "${AnnoyModelFetcher.DimensionKey}": 10
                     | }
                     |}""".stripMargin
    options
  }
  def labelImport(labelName: String, indexPath: String, dictPath: String): Label = {
    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "movie", "string", Seq(Prop("age", "0", "int", true)))

    val options = annoyLabelOptions(indexPath, dictPath)

    Label.findByName(labelName, useCache = false).foreach { label => Label.delete(label.id.get) }

    val label = management.createLabel(
      labelName,
      serviceColumn,
      serviceColumn,
      true,
      service.serviceName,
      Seq.empty[Index].asJava,
      Seq.empty[Prop].asJava,
      "strong",
      null,
      -1,
      "v3",
      "gz",
      options
    )

    val config = ConfigFactory.parseString(options)
    val importerFuture = graph.modelManager.importModel(label, config)(ExecutionContext.Implicits.global)
    Await.result(importerFuture, Duration("3 minutes"))

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

  test("ALS ModelProcess and AnnoyIndexBuildSink") {
    import spark.sqlContext.implicits._

    val inputPath = "/Users/shon/Workspace/incubator-s2graph/example/movielens/input/ratings.csv"
    val indexPath = "./annoy_result"
    val dictPath = "./example/movielens/input/movie.dict"

    buildALS(inputPath, indexPath)

    val labelName = "annoy_index_test"
    val label = labelImport(labelName, indexPath, dictPath)
//    val options = annoyLabelOptions(indexPath, dictPath)
//
//    val config = ConfigFactory.parseString(label.options.get).getConfig("fetcher")
//    val config = ConfigFactory.parseString(options).getConfig("fetcher")

//    val ANNIndexWithDict(index, dict) = AnnoyModelFetcher.buildIndex(config)
//    val v = index.getItemVector(1)
//
//    import scala.collection.JavaConverters._
//    index.getNearest(v, 10).asScala.foreach { x =>
//      println(x)
//    }


//
    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))

    val vertex = graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "1")
    val queryParam = QueryParam(labelName = labelName, limit = 5)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val stepResult = Await.result(graph.getEdges(query), Duration("60 seconds"))

    stepResult.edgeWithScores.foreach { es =>
      println(es.edge.tgtVertex.innerIdVal)
    }
  }
}
