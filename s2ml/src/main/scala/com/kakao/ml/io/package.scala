package com.kakao.ml

import org.apache.spark.sql.ColumnName

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

}
