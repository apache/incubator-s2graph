package com.kakao.ml.io

import com.kakao.ml.{BaseDataProcessor, Params}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

case class StatParams(showPercentile: Option[Boolean]) extends Params

class SourceDataStat(params: StatParams) extends BaseDataProcessor[SourceData, SourceData](params) {

  override protected def processBlock(sqlContext: SQLContext, input: SourceData): SourceData = {

    val numEvents = input.count

    val (numUsers, numItems) = (input.userActivities, input.itemActivities) match {
      case (Some(userActivities), Some(itemActivities)) =>
        (userActivities.length.toLong, itemActivities.length.toLong)
      case _ =>
        val count = input.sourceDF.select(countDistinct(userCol), countDistinct(itemCol)).collect().head
        (count.getLong(0), count.getLong(1))
    }

    val predecessors = getPredecessors.map(_.id).mkString("[", ", ", "]")
    val fmt = java.text.NumberFormat.getNumberInstance(java.util.Locale.US)
    val density = numEvents.toDouble / numUsers / numItems

    show(s"predecessors: $predecessors")
    show(s"numUsers: ${fmt.format(numUsers)}")
    show(s"numItems: ${fmt.format(numItems)}")
    show(s"numRatings: ${fmt.format(numEvents)}")
    show(f"density: ${density * 100}%.4f%% (max: 100%%)")

    /** show sample users */
    (input.userActivities, input.itemActivities) match {
      case (Some(userActivities), Some(itemActivities)) =>
        val sampleUsers = userActivities.take(10).map(_._1).mkString("[", ", ", "]")
        val sampleItems = itemActivities.take(10).map(_._1).mkString("[", ", ", "]")
        show(s"sampleUsers: $sampleUsers")
        show(s"sampleItems: $sampleItems")
      case _ =>
    }

    show("count by label")
    input.sourceDF.groupBy(labelCol).count().orderBy(countCol.desc).show(100, false)

    params.showPercentile.foreach {
      case false =>
      case true =>
        val (userActivityDF, itemActivityDF) = (input.userActivities, input.itemActivities) match {
          case (Some(userActivities), Some(itemActivities)) =>
            import sqlContext.implicits._
            val sc = sqlContext.sparkContext
            val userActivityDF = sc.parallelize(userActivities).toDF(userColString, countColString)
            val itemActivityDF = sc.parallelize(itemActivities).toDF(itemColString, countColString)
            (userActivityDF, itemActivityDF)
          case _ =>
            (input.sourceDF.groupBy(userCol).count(), input.sourceDF.groupBy(itemCol).count())
        }

        show("Percentile of UserActivity")
        userActivityDF
            .withColumn("ntile", ntile(100) over Window.orderBy(countColString))
            .groupBy("ntile")
            .agg(mean(countCol) cast IntegerType as "value")
            .show(100, false)

        show("Percentile of ItemActivity")
        itemActivityDF
            .withColumn("ntile", ntile(100) over Window.orderBy(countColString))
            .groupBy("ntile")
            .agg(mean(countCol) cast IntegerType as "value")
            .show(100, false)
    }

    input
  }

}
