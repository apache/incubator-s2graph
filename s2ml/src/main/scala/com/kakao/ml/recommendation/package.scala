package com.kakao.ml

import org.apache.spark.sql.ColumnName

package object recommendation {

  val indexedFromColString = "from_i"
  val indexedToColString = "to_i"
  val scoreColString = "score"

  val fromColString = "from"
  val toColString = "to"

  val indexedFromCol = new ColumnName(indexedFromColString)
  val indexedToCol = new ColumnName(indexedToColString)
  val scoreCol = new ColumnName(scoreColString)

  val fromCol = new ColumnName(fromColString)
  val toCol = new ColumnName(toColString)

}
