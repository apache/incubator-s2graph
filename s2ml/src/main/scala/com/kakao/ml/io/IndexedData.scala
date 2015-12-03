package com.kakao.ml.io

import com.kakao.ml.Data
import org.apache.spark.sql.DataFrame

/**
 * @param indexedDF tsCol, labelCol, indexedUserCol, indexedItemCol, confidenceCol
 * @param labelWeightDF labelCol, confidenceCol
 * @param indexedUserDF userCol, indexedUserCol
 * @param indexedItemDF itemCol, indexedItemCol
 */
case class IndexedData(
    indexedDF: DataFrame,
    numIndexedData: Option[Long],
    labelWeightDF: DataFrame,
    indexedUserDF: DataFrame,
    indexedItemDF: DataFrame) extends Data

