package com.kakao.ml.io

import com.kakao.ml.BaseDataProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, SQLContext}
import org.apache.spark.storage.StorageLevel

class Indexer extends BaseDataProcessor[SourceData, IndexedData] {

  val defaultBlockSize = 5e6
  val canBroadcast = 5e5

  override protected def processBlock(sqlContext: SQLContext, input: SourceData): IndexedData = {

    import sqlContext.implicits._

    val labelStringDouble: Seq[(String, Double)] = input.labelWeight.toSeq
    var userStringInt: Array[(String, Int)] = null
    var itemStringInt: Array[(String, Int)] = null

    if(input.userActivities.isEmpty || input.itemActivities.isEmpty) {
      val cube = input.sourceDF
          .select(userCol, itemCol)
          .cube(userCol, itemCol)
          .count()
          .where((userCol.isNotNull and itemCol.isNull) or (itemCol.isNotNull and userCol.isNull))
          .select(userCol, itemCol)
          .collect()

      val users = cube.filter(_.isNullAt(1)).map(x => x.getString(0))
      val items = cube.filter(_.isNullAt(0)).map(x => x.getString(1))

      userStringInt = users.zipWithIndex
      itemStringInt = items.zipWithIndex

    } else {
      userStringInt = input.userActivities.get.map(_._1).zipWithIndex
      itemStringInt = input.itemActivities.get.map(_._1).zipWithIndex
    }

    val tmpLabelColString = "label_s"
    val tmpUserColString = "user_s"
    val tmpItemColString = "item_s"
    val tmpLabelCol = new ColumnName(tmpLabelColString)
    val tmpUserCol = new ColumnName(tmpUserColString)
    val tmpItemCol = new ColumnName(tmpItemColString)

    /**
     * using the driver memory for I/O efficiency
     *
     * NOTE: broadcast() on Spark 1.5.1 does not seem to support
     *    inner equi-join using `def join(right: DataFrame, usingColumn: String)`
     * so used tmp{Foo}ColStrings instead */
    var labelDF = sqlContext.sparkContext.parallelize(labelStringDouble, 1)
        .toDF(tmpLabelColString, confidenceColString)
    var userDF = sqlContext.sparkContext.parallelize(userStringInt)
        .toDF(tmpUserColString, indexedUserColString)
    var itemDF = sqlContext.sparkContext.parallelize(itemStringInt)
        .toDF(tmpItemColString, indexedItemColString)

    if (labelStringDouble.length <= canBroadcast)
      labelDF = broadcast(labelDF)
    if (userStringInt.length <= canBroadcast)
      userDF = broadcast(userDF)
    if (itemStringInt.length <= canBroadcast)
      itemDF = broadcast(itemDF)

    val indexedDF = input.sourceDF
        .join(labelDF, labelCol === tmpLabelCol)
        .join(userDF, userCol === tmpUserCol)
        .join(itemDF, itemCol === tmpItemCol)
        .select(tsColString, labelColString, indexedUserColString, indexedItemColString, confidenceColString)

    // materialize
    indexedDF.persist(StorageLevel.MEMORY_AND_DISK)
    val numIndexedData = indexedDF.count()

    IndexedData(indexedDF,
      Some(numIndexedData),
      labelDF.withColumnRenamed(tmpLabelColString, labelColString),
      userDF.withColumnRenamed(tmpUserColString, userColString),
      itemDF.withColumnRenamed(tmpItemColString, itemColString))
  }

}

