package com.kakao.ml.io

import com.kakao.ml.Data
import org.apache.spark.sql.DataFrame

/**
 * @param sourceDF tsCol, labelCol, userCol, itemCol
 */
case class SourceData(
    sourceDF: DataFrame,
    count: Long,
    tsFrom: Long,
    tsTo: Long,
    labelWeight: Map[String, Double],
    userActivities: Option[Array[(String, Int)]] = None,
    itemActivities: Option[Array[(String, Int)]] = None) extends Data
