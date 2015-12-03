package com.kakao.ml.recommendation

import com.github.fommil.netlib.F2jBLAS
import com.kakao.ml.io._
import com.kakao.ml.util.PrivateMethodAccessor
import com.kakao.ml.{BaseDataProcessor, Data, Params}
import com.thesamet.spatial.{DimensionalOrdering, KDTreeMap, Metric}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
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
  val defaultStrategy = "default"

  def copyToUnitVector(factors: RDD[(Int, Array[Double])]): RDD[(Int, Array[Double])] = {
    val unitVecId = factors.mapValues { factor =>
      /** clone due to mutation */
      val v = factor.clone()
      var norm = 0.0
      var i = 0
      while (i < v.length) {
        norm += v(i) * v(i)
        i += 1
      }
      norm = math.sqrt(norm)
      if (norm > Blas.tolerance)
        Blas.blas.dscal(v.length, 1.0 / norm, v, 1)
      v
    }
    unitVecId
  }

  def computeCosineSimilarity(k: Int,
      vecSize: Int, vectors: RDD[(Int, Array[Double])],
      strategy: String): RDD[(Int, Array[(Int, Double)])] = {

    val numBlocks = params.numBlocks.getOrElse(defaultNumBlocks)

    val unitVectors = vectors.partitions.length match {
      case n if n > numBlocks => copyToUnitVector(vectors).coalesce(numBlocks)
      case _ => copyToUnitVector(vectors)
    }

    // materialize
    unitVectors.persist(StorageLevel.MEMORY_AND_DISK)
    val numVectors = unitVectors.count()

    strategy match {
      case "bruteforce" =>
        /** dot product using MatrixFactorizationModel.recommendForAll */
        val dotProduct = "org$apache$spark$mllib$recommendation$MatrixFactorizationModel$$recommendForAll"
        val included = PrivateMethodAccessor(MatrixFactorizationModel, dotProduct)[RDD[(Int, Array[(Int, Double)])]](vecSize, unitVectors, unitVectors, k + 1)
        val excluded = included.map { case (key, values) => key -> values.filter(_._1 != key) }
        excluded
      case  `defaultStrategy` | "kdtree" =>
        CosineSimilarityByKDTree.dotProduct(k, vecSize, unitVectors)
      case "kmeans" =>
        CosineSimilarityByKMeans.dotProduct(k, vecSize, numVectors, unitVectors)
     case s =>
        throw new IllegalArgumentException(s"$s is not supported")
    }

  }

  override def processBlock(sqlContext: SQLContext, input: SimilarityInputData): SimilarityData = {

    import sqlContext.implicits._

    val k = math.max(params.k, 1)
    val strategy = params.strategy.getOrElse(defaultStrategy)

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

object CosineSimilarityByKDTree {

  def dimensionalOrderingForArray(dim: Int)(implicit ord: Ordering[Double]): DimensionalOrdering[Array[Double]] =
    new DimensionalOrdering[Array[Double]] {
      val dimensions = dim

      def compareProjection(d: Int)(x: Array[Double], y: Array[Double]) = ord.compare(
        x(d), y(d))
    }

  implicit def metricFromArray(implicit n: Numeric[Double]): Metric[Array[Double], Double] = new Metric[Array[Double], Double] {
    override def distance(x: Array[Double], y: Array[Double]): Double = {
      var diff = 0.0
      var sum = 0.0
      var i = 0
      while (i < x.length) {
        diff = x(i) - y(i)
        sum += diff * diff
        i += 1
      }
      sum
    }

    override def planarDistance(dimension: Int)(x: Array[Double], y: Array[Double]): Double = {
      val dd = x(dimension) - y(dimension)
      dd * dd
    }
  }

  val blockSize = 4096
  val treeBlockSize = blockSize * 10

  def dotProduct(k: Int, d: Int, vectors: RDD[(Int, Array[Double])]): RDD[(Int, Array[(Int, Double)])] = {

    val srcBlocks = vectors.mapPartitions(_.grouped(blockSize))
    val indexedBlocks = vectors.mapPartitions { iter =>
      iter.grouped(treeBlockSize).map { dstBlock =>
        KDTreeMap.fromSeq(dstBlock.map(_.swap))(dimensionalOrderingForArray(d))
      }
    }

    val product = srcBlocks.cartesian(indexedBlocks).flatMap {
      case (srcBlock, indexedBlock) =>
        srcBlock.flatMap { case (si, sv) =>
          val n = sv.length
          indexedBlock.findNearest(sv, k + 1)
              .flatMap {
                case (dv, di) if si != di =>
                  val dot = Blas.blas.ddot(n, sv, 1, dv, 1)
                  Some((si, (di, dot)))
                case _ => None
              }
        }
    }

    product.topByKey(k)(Ordering.by(_._2))
  }

  def dotProductLocal(k: Int, d: Int, vectors: Seq[(Int, Array[Double])]): Seq[(Int, Array[(Int, Double)])] = {

    val srcBlock = vectors
    val indexedBlock = KDTreeMap.fromSeq(vectors.map(_.swap))(dimensionalOrderingForArray(d))

    srcBlock.map { case (si, sv) =>
      val n = sv.length
      si -> indexedBlock.findNearest(sv, k + 1)
          .flatMap {
            case (dv, di) if si != di =>
              val dot = Blas.blas.ddot(n, sv, 1, dv, 1)
              Some(di, dot)
            case _ => None
          }
          .toArray
    }
  }

}

object CosineSimilarityByKMeans {

  val blockSize = 4096

  def dotProduct(k: Int, d: Int, numVectors: Long, vectors: RDD[(Int, Array[Double])]): RDD[(Int, Array[(Int, Double)])] = {

    val numClusters = math.max(2, (numVectors / blockSize.toDouble).toInt)

    println(s"numVectors: $numVectors")
    println(s"numClusters: $numClusters")

    val v = vectors.map(x => Vectors.dense(x._2))
    val model = KMeans.train(v, numClusters, 10)

    model.predict(v).zip(vectors).groupByKey().flatMap { case (clusterId, vectorsInCluster) =>
      CosineSimilarityByKDTree.dotProductLocal(k, d, vectorsInCluster.toSeq)
    }
  }

}

object Blas {
  
  val tolerance: Double = {
    var tol = 1.0
    while ((1.0 + (tol / 2.0)) != 1.0) {
      tol /= 2.0
    }
    tol
  }
  
  val blas = new F2jBLAS
}
