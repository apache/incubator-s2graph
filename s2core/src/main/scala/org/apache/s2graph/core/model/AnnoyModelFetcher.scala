package org.apache.s2graph.core.model

import annoy4s.Converters.KeyConverter
import annoy4s._
import com.typesafe.config.Config
import org.apache.s2graph.core._
import org.apache.s2graph.core.model.AnnoyModelFetcher.IndexFilePathKey
import org.apache.s2graph.core.types.VertexId

import scala.concurrent.{ExecutionContext, Future}

object AnnoyModelFetcher {
  val IndexFilePathKey = "annoyIndexFilePath"
  val DictFilePathKey = "annoyDictFilePath"
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

class AnnoyModelFetcher(val graph: S2GraphLike) extends Fetcher {
  val builder = graph.elementBuilder

  //  var model: ANNIndexWithDict = _
  var model: Annoy[String] = _

  override def init(config: Config)(implicit ec: ExecutionContext): Future[Fetcher] = {
    Future {
      model = AnnoyModelFetcher.buildAnnoy4s(config.getString(IndexFilePathKey))
      //        AnnoyModelFetcher.buildIndex(config)

      this
    }
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

        val edge = graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction)

        EdgeWithScore(edge, score, queryParam.label)
      }

      StepResult(edgeWithScores, Nil, Nil)
      //
      //      val srcIndexOpt = model.dictRev.get(vertex.innerId.toIdString())
      //
      //      srcIndexOpt.map { srcIdx =>
      //        val srcVector = model.index.getItemVector(srcIdx)
      //        val nns = model.index.getNearest(srcVector, queryParam.limit).asScala
      //
      //        val edges = nns.map { tgtIdx =>
      //          val tgtVertexId = builder.newVertexId(queryParam.label.service,
      //            queryParam.label.tgtColumnWithDir(queryParam.labelWithDir.dir), model.dict(tgtIdx))
      //
      //          graph.toEdge(vertex.innerId.value, tgtVertexId.innerId.value, queryParam.labelName, queryParam.direction)
      //        }
      //        val edgeWithScores = edges.map(e => EdgeWithScore(e, 1.0, queryParam.label))
      //        StepResult(edgeWithScores, Nil, Nil)
      //      }.getOrElse(StepResult.Empty)
    }

    Future.successful(stepResultLs)
  }

  override def close(): Unit = {
    // do clean up
  }
}
