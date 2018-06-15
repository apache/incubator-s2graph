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

package org.apache.s2graph.core.fetcher.tensorflow

import java.net.URL
import java.nio.file.Paths

import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}


object InceptionFetcher {
  val ModelPath = "modelPath"

  def getImageBytes(urlText: String): Array[Byte] = {
    val url = new URL(urlText)

    IOUtils.toByteArray(url)
  }

  def predict(graphDef: Array[Byte],
              labels: Seq[String])(imageBytes: Array[Byte], topK: Int = 10): Seq[(String, Float)] = {
    try {
      val image = LabelImage.constructAndExecuteGraphToNormalizeImage(imageBytes)
      try {
        val labelProbabilities = LabelImage.executeInceptionGraph(graphDef, image)
        val topKIndices = labelProbabilities.zipWithIndex.sortBy(_._1).reverse
          .take(Math.min(labelProbabilities.length, topK)).map(_._2)

        val ls = topKIndices.map { idx => (labels(idx), labelProbabilities(idx)) }

        ls
      } catch {
        case e: Throwable => Nil
      } finally if (image != null) image.close()
    }
  }
}

class InceptionFetcher(graph: S2GraphLike) extends EdgeFetcher {

  import InceptionFetcher._

  import scala.collection.JavaConverters._
  import org.apache.s2graph.core.TraversalHelper._
  val builder = graph.elementBuilder

  var graphDef: Array[Byte] = _
  var labels: Seq[String] = _

  override def init(config: Config)(implicit ec: ExecutionContext): Unit = {
    val modelPath = config.getString(ModelPath)
    graphDef = LabelImage.readAllBytesOrExit(Paths.get(modelPath, "tensorflow_inception_graph.pb"))
    labels = LabelImage.readAllLinesOrExit(Paths.get(modelPath, "imagenet_comp_graph_label_strings.txt")).asScala
  }

  override def close(): Unit = {}

  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val vertex = queryRequest.vertex
      val queryParam = queryRequest.queryParam
      val shouldBuildParents = queryRequest.query.queryOption.returnTree || queryParam.whereHasParent
      val parentEdges = if (shouldBuildParents) prevStepEdges.getOrElse(queryRequest.vertex.id, Nil) else Nil

      val urlText = vertex.innerId.toIdString()

      val edgeWithScores = predict(graphDef, labels)(getImageBytes(urlText), queryParam.limit).flatMap { case (label, score) =>
        val tgtVertexId = builder.newVertexId(queryParam.label.service,
          queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), label)

        val props: Map[String, Any] = if (queryParam.label.metaPropsInvMap.contains("score")) Map("score" -> score) else Map.empty
        val edge = graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction, props = props)

        edgeToEdgeWithScore(queryRequest, edge, parentEdges)
      }

      StepResult(edgeWithScores, Nil, Nil)
    }

    Future.successful(stepResultLs)
  }

  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] =
    Future.successful(Nil)
}
