package org.apache.s2graph.core.fetcher.tensorflow

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.s2graph.core.fetcher.BaseFetcherTest
import play.api.libs.json.Json

class InceptionFetcherTest extends BaseFetcherTest {
  val runDownloadModel: Boolean = false
  val runCleanup: Boolean = false

  def cleanup(downloadPath: String, dir: String) = {
    synchronized {
      FileUtils.deleteQuietly(new File(downloadPath))
      FileUtils.deleteDirectory(new File(dir))
    }
  }
  def downloadModel(dir: String) = {
    import sys.process._
    synchronized {
      FileUtils.forceMkdir(new File(dir))

      val url = "https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip"
      val wget = s"wget $url"
      wget !
      val unzip = s"unzip inception5h.zip -d $dir"
      unzip !
    }
  }

  test("test get bytes for image url") {
    val downloadPath = "inception5h.zip"
    val modelPath = "inception"
    try {
      if (runDownloadModel) downloadModel(modelPath)

      val serviceName = "s2graph"
      val columnName = "user"
      val labelName = "image_net"
      val options =
        s"""
           |{
           |  "fetcher": {
           |    "className": "org.apache.s2graph.core.fetcher.tensorflow.InceptionFetcher",
           |    "modelPath": "$modelPath"
           |  }
           |}
       """.stripMargin
      val (service, column, label) = initEdgeFetcher(serviceName, columnName, labelName, Option(options))

      val srcVertices = Seq(
//        "http://www.gstatic.com/webp/gallery/1.jpg",
//        "http://www.gstatic.com/webp/gallery/2.jpg",
//        "http://www.gstatic.com/webp/gallery/3.jpg"
//        "https://di2ponv0v5otw.cloudfront.net/posts/2018/04/16/5ad59a6a61ca107f50032b40/m_5ad59a7750687c9f91641d8b.jpg"
        "https://t1.daumcdn.net/news/201805/14/autonnews/20180514082041618njfp.jpg"
      )
      val stepResult = queryEdgeFetcher(service, column, label, srcVertices)

      stepResult.edgeWithScores.groupBy(_.edge.srcVertex).foreach { case (srcVertex, ls) =>
        val url = srcVertex.innerIdVal.toString
        val scores = ls.map { es =>
          val edge = es.edge
          val label = edge.tgtVertex.innerIdVal.toString
          val score = edge.property[Double]("score").value()

          Json.obj("label" -> label, "score" -> score)
        }
        val jsArr = Json.toJson(scores)
        val json = Json.obj("url" -> url, "scores" -> jsArr)
        println(Json.prettyPrint(json))
      }
    } finally {
      if (runCleanup) cleanup(downloadPath, modelPath)
    }
  }
}
