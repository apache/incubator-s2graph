package com.kakao.ml

import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.types.{StringType, LongType, StructField, StructType}

package object io {

  val tsColString = "ts"
  val labelColString = "label"
  val userColString = "user"
  val itemColString = "item"
  val countColString = "count"

  val indexedUserColString = "user_i"
  val indexedItemColString = "item_i"
  val confidenceColString = "confidence"

  val tsCol = new ColumnName(tsColString)
  val labelCol = new ColumnName(labelColString)
  val userCol = new ColumnName(userColString)
  val itemCol = new ColumnName(itemColString)
  val countCol = new ColumnName(countColString)
  val indexedUserCol = new ColumnName(indexedUserColString)
  val indexedItemCol = new ColumnName(indexedItemColString)
  val confidenceCol = new ColumnName(confidenceColString)

  val rawSchema = StructType(Seq(
    StructField("log_ts", LongType),
    StructField("operation", StringType),
    StructField("log_type", StringType),
    StructField("edge_from", StringType),
    StructField("edge_to", StringType),
    StructField("label", StringType),
    StructField("props", StringType),
    StructField("service", StringType),
    StructField("date_id", StringType)))

}
