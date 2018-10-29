package org.apache.s2graph.s2jobs.wal.process.params

import org.apache.s2graph.s2jobs.task.TaskConf

object AggregateParam {
  val defaultGroupByKeys = Seq("from")
  val defaultTopK = 1000
  val defaultIsArrayType = false
  val defaultShouldSortTopItems = true

  def fromTaskConf(taskConf: TaskConf): AggregateParam = {
    val groupByKeys = taskConf.options.get("groupByKeys").map(_.split(",").filter(_.nonEmpty).toSeq)
    val maxNumOfEdges = taskConf.options.get("maxNumOfEdges").map(_.toInt).getOrElse(defaultTopK)
    val arrayType = taskConf.options.get("arrayType").map(_.toBoolean).getOrElse(defaultIsArrayType)
    val sortTopItems = taskConf.options.get("sortTopItems").map(_.toBoolean).getOrElse(defaultShouldSortTopItems)
    val numOfPartitions = taskConf.options.get("numOfPartitions").map(_.toInt)
    val validTimestampDuration = taskConf.options.get("validTimestampDuration").map(_.toLong).getOrElse(Long.MaxValue)
    val nowOpt = taskConf.options.get("now").map(_.toLong)

    new AggregateParam(groupByKeys = groupByKeys,
      topK = Option(maxNumOfEdges),
      isArrayType = Option(arrayType),
      shouldSortTopItems = Option(sortTopItems),
      numOfPartitions = numOfPartitions,
      validTimestampDuration = Option(validTimestampDuration),
      nowOpt = nowOpt
    )
  }
}

case class AggregateParam(groupByKeys: Option[Seq[String]],
                          topK: Option[Int],
                          isArrayType: Option[Boolean],
                          shouldSortTopItems: Option[Boolean],
                          numOfPartitions: Option[Int],
                          validTimestampDuration: Option[Long],
                          nowOpt: Option[Long]) {

  import AggregateParam._

  val groupByColumns = groupByKeys.getOrElse(defaultGroupByKeys)
  val heapSize = topK.getOrElse(defaultTopK)
  val arrayType = isArrayType.getOrElse(defaultIsArrayType)
  val sortTopItems = shouldSortTopItems.getOrElse(defaultShouldSortTopItems)
  val now = nowOpt.getOrElse(System.currentTimeMillis())
}
