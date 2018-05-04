package org.apache.s2graph.core.model.fasttext

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.Integrate.IntegrateCommon
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.{Query, QueryParam, QueryRequest}
import org.apache.s2graph.core.schema.Label

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

class FastTextFetcherTest extends IntegrateCommon {
  import TestUtil._

  test("FastTextFetcher init test.") {
    val modelPath = "./emoji"
    val config = ConfigFactory.parseMap(Map(FastText.DBPathKey -> modelPath).asJava)
    val fetcher = new FastTextFetcher(graph)
    Await.ready(fetcher.init(config)(ExecutionContext.Implicits.global), Duration("3 minutes"))

    val service = management.createService("s2graph", "localhost", "s2graph_htable", -1, None).get
    val emojiColumn =
      management.createServiceColumn("s2graph", "emoji", "string", Seq(Prop("url", "", "string", false)))

    val sentenceColumn =
      management.createServiceColumn("s2graph", "sentence", "string", Nil)

    val labelName = "sentence_emoji"

    Label.findByName(labelName, useCache = false).foreach { label => Label.delete(label.id.get) }

    val label = management.createLabel(
      labelName,
      sentenceColumn ,
      emojiColumn,
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
    val vertex = graph.elementBuilder.toVertex(service.serviceName, sentenceColumn.columnName, "화났어")
    val queryParam = QueryParam(labelName = labelName, limit = 5)

    val query = Query.toQuery(srcVertices = Seq(vertex), queryParams = Seq(queryParam))
    val queryRequests = Seq(
      QueryRequest(query, 0, vertex, queryParam)
    )

    val future = fetcher.fetches(queryRequests, Map.empty)
    val results = Await.result(future, Duration("10 seconds"))
    results.foreach { stepResult =>
      stepResult.edgeWithScores.foreach { es =>
        val Array(itemId, resourceId) = es.edge.tgtVertex.innerIdVal.toString.replace("__label__", "").split("_")
        val text = String.format("http://item.kakaocdn.net/dw/%s.thum_%03d.png", itemId, Int.box(resourceId.toInt))

        println(text)
      }
    }
  }
}
