package com.kakao.ml.recommendation

import com.kakao.ml.io.IndexedData
import com.kakao.ml.{BaseDataProcessor, Data, Params}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame

case class MatrixFactorizationData(
    model: ALSModel,
    trainedDF: Option[DataFrame]) extends Data

abstract class MatrixFactorization[T <: Params](params: T)
    extends BaseDataProcessor[IndexedData, MatrixFactorizationData](params)

