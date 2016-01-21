package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{Data, EmptyData, Params}
import org.apache.spark.sql.DataFrame

case class QueryParams(query: String) extends Params

case class QueryData(df: DataFrame) extends Data

class Query(params: QueryParams) extends Source[QueryData](params) {
  override protected def processBlock(input: EmptyData): QueryData =
    QueryData(context.sqlContext.sql(params.query))
}
