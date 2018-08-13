package org.apache.s2graph.s2jobs.wal.process.params


object AggregateParam {
  val defaultGroupByKeys = Seq("from")
  val defaultTopK = 1000
  val defaultIsArrayType = false
  val defaultShouldSortTopItems = true
}

case class AggregateParam(groupByKeys: Option[Seq[String]],
                          topK: Option[Int],
                          isArrayType: Option[Boolean],
                          shouldSortTopItems: Option[Boolean]) {

  import AggregateParam._

  val groupByColumns = groupByKeys.getOrElse(defaultGroupByKeys)
  val heapSize = topK.getOrElse(defaultTopK)
  val arrayType = isArrayType.getOrElse(defaultIsArrayType)
  val sortTopItems = shouldSortTopItems.getOrElse(defaultShouldSortTopItems)
}
