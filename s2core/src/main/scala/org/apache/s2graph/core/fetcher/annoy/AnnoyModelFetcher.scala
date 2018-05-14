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

package org.apache.s2graph.core.fetcher.annoy

import annoy4s.Converters.KeyConverter
import annoy4s._
import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}

object AnnoyModelFetcher {
  val IndexFilePathKey = "annoyIndexFilePath"
//  val DictFilePathKey = "annoyDictFilePath"
  val DimensionKey = "annoyIndexDimension"
  val IndexTypeKey = "annoyIndexType"

  //  def loadDictFromLocal(file: File): Map[Int, String] = {
  //    val files = if (file.isDirectory) {
  //      file.listFiles()
  //    } else {
  //      Array(file)
  //    }
  //
  //    files.flatMap { file =>
  //      Source.fromFile(file).getLines().zipWithIndex.flatMap { case (line, _idx) =>
  //        val tokens = line.stripMargin.split(",")
  //        try {
  //          val tpl = if (tokens.length < 2) {
  //            (tokens.head.toInt, tokens.head)
  //          } else {
  //            (tokens.head.toInt, tokens.tail.head)
  //          }
  //          Seq(tpl)
  //        } catch {
  //          case e: Exception => Nil
  //        }
  //      }
  //    }.toMap
  //  }

  def buildAnnoy4s[T](indexPath: String)(implicit converter: KeyConverter[T]): Annoy[T] = {
    Annoy.load[T](indexPath)
  }

  //  def buildIndex(indexPath: String,
  //                 dictPath: String,
  //                 dimension: Int,
  //                 indexType: IndexType): ANNIndexWithDict = {
  //    val dict = loadDictFromLocal(new File(dictPath))
  //    val index = new ANNIndex(dimension, indexPath, indexType)
  //
  //    ANNIndexWithDict(index, dict)
  //  }
  //
  //  def buildIndex(config: Config): ANNIndexWithDict = {
  //    val indexPath = config.getString(IndexFilePathKey)
  //    val dictPath = config.getString(DictFilePathKey)
  //
  //    val dimension = config.getInt(DimensionKey)
  //    val indexType = Try { config.getString(IndexTypeKey) }.toOption.map(IndexType.valueOf).getOrElse(IndexType.ANGULAR)
  //
  //    buildIndex(indexPath, dictPath, dimension, indexType)
  //  }
}

//
//case class ANNIndexWithDict(index: ANNIndex, dict: Map[Int, String]) {
//  val dictRev = dict.map(kv => kv._2 -> kv._1)
//}

class AnnoyModelFetcher(val graph: S2GraphLike) extends EdgeFetcher {
  import AnnoyModelFetcher._

  val builder = graph.elementBuilder

  //  var model: ANNIndexWithDict = _
  var model: Annoy[String] = _

  override def init(config: Config)(implicit ec: ExecutionContext): Unit = {

    model = AnnoyModelFetcher.buildAnnoy4s(config.getString(IndexFilePathKey))
  }

  /** Fetch **/
  override def fetches(queryRequests: Seq[QueryRequest],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])(implicit ec: ExecutionContext): Future[Seq[StepResult]] = {
    val stepResultLs = queryRequests.map { queryRequest =>
      val vertex = queryRequest.vertex
      val queryParam = queryRequest.queryParam

      val edgeWithScores = model.query(vertex.innerId.toIdString(), queryParam.limit).getOrElse(Nil).map { case (tgtId, score) =>
        val tgtVertexId = builder.newVertexId(queryParam.label.service,
          queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), tgtId)

        val props: Map[String, Any] = if (queryParam.label.metaPropsInvMap.contains("score")) Map("score" -> score) else Map.empty
        val edge = graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction, props = props)

        EdgeWithScore(edge, score, queryParam.label)
      }

      StepResult(edgeWithScores, Nil, Nil)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = {
    // do clean up
    model.close
  }

  // not supported yet.
  override def fetchEdgesAll()(implicit ec: ExecutionContext): Future[Seq[S2EdgeLike]] =
    Future.successful(Nil)
}
