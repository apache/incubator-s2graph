package org.apache.s2graph.core.model.fasttext

import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}


class FastTextFetcher(val graph: S2GraphLike) extends Fetcher {
  val builder = graph.elementBuilder
  var fastText: FastText = _

  override def init(config: Config)(implicit ec: ExecutionContext): Future[Fetcher] = {
    Future {
      val dbPath = config.getString(FastText.DBPathKey)

      fastText = new FastText(dbPath)

      this
    }
  }

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val vertex = queryRequest.vertex
      val queryParam = queryRequest.queryParam
      val line = fastText.getLine(vertex.innerId.toIdString())

      val edgeWithScores = fastText.predict(line, queryParam.limit).map { case (_label, score) =>
        val tgtVertexId = builder.newVertexId(queryParam.label.service,
          queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), _label)

        val props: Map[String, Any] = if (queryParam.label.metaPropsInvMap.contains("score")) Map("score" -> score) else Map.empty
        val edge = graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction, props = props)

        EdgeWithScore(edge, score, queryParam.label)
      }

      StepResult(edgeWithScores, Nil, Nil)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = if (fastText != null) fastText.close()
}
