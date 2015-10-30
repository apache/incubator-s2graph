package com.kakao.ml.recommendation

import com.kakao.ml.io._
import com.kakao.ml.util.PrivateMethodAccessor
import com.kakao.ml.{BaseDataProcessor, Data, Params}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

case class SimilarityInputData(
    indexedUserDF: DataFrame,
    indexedItemDF: DataFrame,
    model: ALSModel) extends Data

case class CosineSimilarityParams(
    target: String, k: Int, numBlocks: Option[Int], strategy: Option[String]) extends Params

/**
 * @param similarUserDF fromCol, toCol, scoreCol
 * @param similarItemDF fromCol, toCol, scoreCol
 */
case class SimilarityData(
    similarUserDF: Option[DataFrame],
    similarItemDF: Option[DataFrame]) extends Data

class CosineSimilarity(params: CosineSimilarityParams)
    extends BaseDataProcessor[SimilarityInputData, SimilarityData](params) {

  val defaultNumBlocks = 20
  val defaultStrategy = "bruteforce"

  def copyToUnitVector(factors: RDD[(Int, Array[Double])]): RDD[(Int, Array[Double])] = {
    val unitVecId = factors.mapValues { factor =>
      val v = Vectors.dense(factor.clone())
      val norm = Vectors.norm(v, 2.0)
      if (norm > 1) Linalg.scal(1.0 / norm, v)
      v.toArray
    }
    unitVecId
  }

  def computeCosineSimilarity(k: Int,
      vecSize: Int, vectors: RDD[(Int, Array[Double])],
      strategy: String): RDD[(Int, Array[(Int, Double)])] = {

    val numBlocks = params.numBlocks.getOrElse(defaultNumBlocks)

    val unitVectors = copyToUnitVector(vectors).repartition(numBlocks)

    // materialize
    unitVectors.persist(StorageLevel.MEMORY_AND_DISK)
    unitVectors.count()

    strategy match {
      case "bruteforce" =>
        /* dot product using MatrixFactorizationModel.recommendForAll */
        val dotProduct = "org$apache$spark$mllib$recommendation$MatrixFactorizationModel$$recommendForAll"
        PrivateMethodAccessor(MatrixFactorizationModel, dotProduct)[RDD[(Int, Array[(Int, Double)])]](vecSize, unitVectors, unitVectors, k)
      case s =>
        throw new IllegalArgumentException(s"$s is not supported")
    }

  }

  override def processBlock(sqlContext: SQLContext, input: SimilarityInputData): SimilarityData = {

    import sqlContext.implicits._

    val k = math.max(params.k, 1)
    val strategy = params.strategy.getOrElse("bruteforce")

    val rank = input.model.rank
    val (df, indexingMap, sCol, iCol) = params.target match {
      case "user" =>
        val df = input.model.userFactors
        (df, input.indexedUserDF, userCol, indexedUserCol)
      case "item" =>
        val df = input.model.itemFactors
        (df, input.indexedItemDF, itemCol, indexedItemCol)
    }

    val vectorsWithId = df.map { case Row(id: Int, features: Seq[_]) =>
      (id, features.asInstanceOf[Seq[Float]].map(_.toDouble).toArray)
    }

    val similarities = computeCosineSimilarity(k, rank, vectorsWithId, strategy)

    val indexedSimilarityDF = similarities.flatMap { case (from, arr) =>
      arr.map { case (to, score) => (from, to, score) }
    }.toDF(indexedFromColString, indexedToColString, scoreColString)

    val similarityDF = indexedSimilarityDF
        .join(broadcast(indexingMap), indexedFromCol === iCol)
        .select(sCol as fromColString, indexedToCol, scoreCol)
        .join(broadcast(indexingMap), indexedToCol === iCol)
        .select(fromCol, sCol as toColString, scoreCol)

    /** materialize */
    similarityDF.persist(StorageLevel.MEMORY_AND_DISK)
    similarityDF.count()

    params.target match {
      case "user" =>
        SimilarityData(Some(similarityDF), None)
      case "item" =>
        SimilarityData(None, Some(similarityDF))
    }
  }
}
