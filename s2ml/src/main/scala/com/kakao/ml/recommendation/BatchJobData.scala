package com.kakao.ml.recommendation

import com.kakao.ml.Data
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame

case class BatchJobData(
    tsFrom: Long,
    tsTo: Long,
    indexedUserDF: Option[DataFrame],
    indexedItemDF: Option[DataFrame],
    model: Option[ALSModel],
    similarUserDF: Option[DataFrame],
    similarItemDF: Option[DataFrame]) extends Data

case class LastBatchJobData(
    lastTsFrom: Long,
    lastTsTo: Long,
    indexedUserDF: Option[DataFrame],
    indexedItemDF: Option[DataFrame],
    model: Option[ALSModel],
    similarUserDF: Option[DataFrame],
    similarItemDF: Option[DataFrame]) extends Data
