package com.kakao.ml.recommendation

import com.kakao.ml.launcher.{Launcher, LocalSparkContext}
import com.kakao.ml.{BaseDataProcessor, EmptyData}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSuite, Matchers}

object StaticSimilarityData {

  val numSamples = 1000
  val vecSize = 10

  val rnd = new scala.util.Random()
  val arr = Array.tabulate(numSamples)(i => i -> Array.tabulate(vecSize)(_ => rnd.nextFloat()))
  val indexMap = (0 until numSamples).map(i => i.toString -> i)

}

class CosineSimilarityDataGenerator extends BaseDataProcessor[EmptyData, SimilarityInputData] {

  override protected def processBlock(sqlContext: SQLContext, input: EmptyData): SimilarityInputData = {

    import sqlContext.implicits._
    import com.kakao.ml.io._

    val fakeFactors = sqlContext.sparkContext.parallelize(StaticSimilarityData.arr).toDF("id", "features").cache()

    /** to access the private constructor */
    val model = Class.forName("org.apache.spark.ml.recommendation.ALSModel")
        .getConstructors
        .head
        .newInstance("", Integer.valueOf(StaticSimilarityData.vecSize), fakeFactors, fakeFactors)
        .asInstanceOf[ALSModel]

    val userMap = sqlContext.sparkContext.parallelize(StaticSimilarityData.indexMap)
        .toDF(userColString, indexedUserColString)
        .cache()

    val itemMap = sqlContext.sparkContext.parallelize(StaticSimilarityData.indexMap)
        .toDF(itemColString, indexedItemColString)
        .cache()

    SimilarityInputData(userMap, itemMap, model)
  }
}

class CosineSimilarityTest extends FunSuite with Matchers with LocalSparkContext {

  val sampleJson =
    """
      |{
      |  "name": "cossim",
      |  "processors": [
      |    {
      |      "id": "gen",
      |      "class": "com.kakao.ml.recommendation.CosineSimilarityDataGenerator"
      |    },
      |    {
      |      "id": "sim0",
      |      "pid": "gen",
      |      "class": "com.kakao.ml.recommendation.CosineSimilarity",
      |      "params": {
      |        "numBlocks": 4,
      |        "target": "item",
      |        "k": %d,
      |        "strategy": "%s"
      |      }
      |    }
      |  ]
      |}
      |
    """.stripMargin

  test("the results of bruteforce and kdtree are the same") {

    val k = 10

    val bruteForcePlan = Launcher.buildPipeline(sampleJson.format(k, "bruteforce"))
    val bruteForceOutput = bruteForcePlan.head.asInstanceOf[CosineSimilarity].process(new SQLContext(sc))

    val kdTreePlan = Launcher.buildPipeline(sampleJson.format(k, "kdtree"))
    val kdTreeOutput = kdTreePlan.head.asInstanceOf[CosineSimilarity].process(new SQLContext(sc))

    val bruteForceResults = bruteForceOutput.similarItemDF.get
    val kdTreeResults = kdTreeOutput.similarItemDF.get

    val n = bruteForceResults.count()

    kdTreeResults.count() should be (n)

    val joined = bruteForceResults.join(kdTreeResults, Seq("from", "to")).cache()

    joined.count() should be (n)

    val numDiff = joined.map(r => math.abs(r.getDouble(2) - r.getDouble(3))).filter(_ > Blas.tolerance).count()

    numDiff should be (0L)

    val a = bruteForceResults.map(r => r.getString(0) -> (r.getString(1), r.getDouble(2))).groupByKey()
    val b = kdTreeResults.map(r => r.getString(0) -> (r.getString(1), r.getDouble(2))).groupByKey()

    val map = a.join(b)
        .map { case (_, (tr, te)) =>
          ContinuousEvaluator.getAveragePrecision(
            te.toSeq.sortBy(-_._2).map(_._1).toArray,
            tr.toSeq.sortBy(-_._2).map(_._1).toArray
          )
        }
        .mean()

    map should be (1.0)
  }

  test("the results of bruteforce and kmeans are close") {

    val k = 10

    val bruteForcePlan = Launcher.buildPipeline(sampleJson.format(k, "bruteforce"))
    val bruteForceOutput = bruteForcePlan.head.asInstanceOf[CosineSimilarity].process(new SQLContext(sc))

    val kdTreePlan = Launcher.buildPipeline(sampleJson.format(k, "kmeans"))
    val kdTreeOutput = kdTreePlan.head.asInstanceOf[CosineSimilarity].process(new SQLContext(sc))

    val bruteForceResults = bruteForceOutput.similarItemDF.get
    val kmeansResults = kdTreeOutput.similarItemDF.get

    val n = bruteForceResults.count()

    kmeansResults.count() should be (n)

    val a = bruteForceResults.map(r => r.getString(0) -> (r.getString(1), r.getDouble(2))).groupByKey()
    val b = kmeansResults.map(r => r.getString(0) -> (r.getString(1), r.getDouble(2))).groupByKey()

    val map = a.join(b)
        .map { case (_, (tr, te)) =>
          ContinuousEvaluator.getAveragePrecision(
            te.toSeq.sortBy(-_._2).map(_._1).toArray,
            tr.toSeq.sortBy(-_._2).map(_._1).toArray
          )
        }
        .mean()

    println(s"map: $map")

    map should be > 0.7
  }

}
