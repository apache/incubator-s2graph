package com.kakao.ml.io

import com.kakao.ml.{BaseDataProcessor, Params}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

case class SourceFilterParams(
    userLowerBound: Option[Long],
    userUpperBound: Option[Long],
    itemLowerBound: Option[Long],
    itemUpperBound: Option[Long],
    strategy: Option[String]) extends Params

class SourceFilter(params: SourceFilterParams)
    extends BaseDataProcessor[SourceData, SourceData](params) {

  val defaultStrategy = "default"

  override def processBlock(sqlContext: SQLContext, input: SourceData): SourceData = {

    val userLowerBound = math.max(2, params.userLowerBound.getOrElse(0L))
    val userUpperBound = math.max(userLowerBound, params.userUpperBound.getOrElse(Long.MaxValue))
    val itemLowerBound = math.max(2, params.itemLowerBound.getOrElse(0L))
    val itemUpperBound = math.max(itemLowerBound, params.itemUpperBound.getOrElse(Long.MaxValue))
    val strategy = params.strategy.getOrElse(defaultStrategy)

    show(s"userLowerBound: $userLowerBound")
    show(s"userUpperBound: $userUpperBound")
    show(s"itemLowerBound: $itemLowerBound")
    show(s"itemUpperBound: $itemUpperBound")
    show(s"strategy: $strategy")

    /**
     * cube(userCol, itemCol).count() generates
     *   user, null, count
     *   null, item, count
     *   user, item, count
     */
    val cubeCount = input.sourceDF
        .select(userCol, itemCol)
        .cube(userCol, itemCol)
        .count()

    strategy match {
      case `defaultStrategy` | "whitelist" =>
        /**
         * this requires sufficient driver memory to store all userString and itemString.
         */
        val userItemCount = cubeCount
            .where((userCol.isNotNull and itemCol.isNull and countCol.between(userLowerBound, userUpperBound))
                or (itemCol.isNotNull and userCol.isNull and countCol.between(itemLowerBound, itemUpperBound)))
            .select(userCol, itemCol, countCol)
            .collect()

        val userActivities = userItemCount.filter(_.isNullAt(1)).map(x => x.getString(0) -> x.getLong(2).toInt)
        val itemActivities = userItemCount.filter(_.isNullAt(0)).map(x => x.getString(1) -> x.getLong(2).toInt)

        /** TODO: efficient? */
        val newSourceDF = input.sourceDF
            .where(userCol.isin(userActivities.map(_._1): _*) and itemCol.isin(itemActivities.map(_._1): _*))

        /** materialize */
        newSourceDF.persist(StorageLevel.MEMORY_AND_DISK)
        val (tsCount, tsFrom, tsTo) = newSourceDF.select(count(tsCol), min(tsCol), max(tsCol)).head(1).map { row =>
          (row.getLong(0), row.getLong(1), row.getLong(2))
        }.head

        SourceData(newSourceDF, tsCount, tsFrom, tsTo, input.labelWeight, Some(userActivities), Some(itemActivities))

      case "join" =>
        val userItemCount = cubeCount
            .where((userCol.isNotNull and itemCol.isNull and countCol.between(userLowerBound, userUpperBound))
                or (itemCol.isNotNull and userCol.isNull and countCol.between(itemLowerBound, itemUpperBound)))
            .select(userCol, itemCol, countCol)

        val userWhitelist = userItemCount.where(itemCol isNull).select(userCol)
        val itemWhitelist = userItemCount.where(userCol isNull).select(itemCol)

        val newSourceDF = input.sourceDF
            .join(userWhitelist, userColString)
            .join(itemWhitelist, itemColString)
            .select(tsCol, labelCol, userCol, itemCol)

        /** materialize */
        newSourceDF.persist(StorageLevel.MEMORY_AND_DISK)
        val (tsCount, tsFrom, tsTo) = newSourceDF.select(count(tsCol), min(tsCol), max(tsCol)).head(1).map { row =>
          (row.getLong(0), row.getLong(1), row.getLong(2))
        }.head

        SourceData(newSourceDF, tsCount, tsFrom, tsTo, input.labelWeight, None, None)
    }
  }

}

