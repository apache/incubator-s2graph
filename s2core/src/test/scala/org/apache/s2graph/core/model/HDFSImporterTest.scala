package org.apache.s2graph.core.model

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.{Query, QueryParam}
import org.apache.s2graph.core.schema.Label

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class HDFSImporterTest extends IntegrateCommon {
  import scala.collection.JavaConverters._

  test("hdfs test.") {

    val labelName = "hdfs_importer_test"
    val HDFS_CONF_DIR = "./"

    val remoteIndexFilePath = getClass.getResource(s"/test-index.tree").toURI.getPath
    val remoteDictFilePath = getClass.getResource(s"/test-index.dict").toURI.getPath

    val localIndexFilePath = ".test-index.tree"
    val localDictFilePath = ".test-index.dict"

    FileUtils.deleteQuietly(new File(localIndexFilePath))
    FileUtils.deleteQuietly(new File(localDictFilePath))

    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val serviceColumn =
      management.createServiceColumn("s2graph", "user", "string", Seq(Prop("age", "0", "int", true)))

    val options = s"""{
                     | "importer": {
                     |   "${ModelManager.ImporterClassNameKey}": "org.apache.s2graph.core.model.HDFSImporter",
                     |   "${HDFSImporter.HDFSConfDirKey}": "$HDFS_CONF_DIR",
                     |   "${HDFSImporter.PathsKey}": [{
                     |       "src":  "${remoteIndexFilePath}",
                     |       "tgt": "${localIndexFilePath}"
                     |     }, {
                     |       "src": "${remoteDictFilePath}",
                     |       "tgt": "${localDictFilePath}"
                     |     }
                     |   ]
                     | },
                     | "fetcher": {
                     |   "${ModelManager.FetcherClassNameKey}": "org.apache.s2graph.core.model.MemoryModelFetcher"
                     | }
                     |}""".stripMargin

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
      ""
    )

    Label.updateOption(label.label, options)

    println("*" * 80)
    println(Label.findByName(label.label, false).get.toFetcherConfig)

    val config = ConfigFactory.parseString(options)
    val importerFuture = graph.modelManager.importModel(label, config)(ExecutionContext.Implicits.global)
    Await.result(importerFuture, Duration("3 minutes"))

    Thread.sleep(5000)

    val vertex = graph.elementBuilder.toVertex(service.serviceName, serviceColumn.columnName, "0")
    val queryParam = QueryParam(labelName = labelName, limit = 5)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val stepResult = Await.result(graph.getEdges(query), Duration("60 seconds"))

    stepResult.edgeWithScores.foreach { es =>
      println(es.edge)
    }

    FileUtils.deleteQuietly(new File(localIndexFilePath))
    FileUtils.deleteQuietly(new File(localDictFilePath))
  }
}
