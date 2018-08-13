package org.apache.s2graph.s2jobs.wal.process

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.{WalLog, WalLogAgg}
import org.apache.spark.sql._

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

object WalLogAggregateProcess {
  def aggregate(ss: SparkSession,
                dataset: Dataset[WalLogAgg],
                aggregateParam: AggregateParam)(implicit ord: Ordering[WalLog]) = {
    import ss.implicits._
    dataset.groupByKey(_.from).mapGroups { case (_, iter) =>
      WalLogAgg.merge(iter, aggregateParam)
    }.toDF(WalLogAgg.outputColumns: _*)
  }

  def aggregateRaw(ss: SparkSession,
                   dataset: Dataset[WalLog],
                   aggregateParam: AggregateParam)(implicit ord: Ordering[WalLog]): DataFrame = {
    import ss.implicits._

    dataset.groupByKey(walLog => walLog.from).mapGroups { case (key, iter) =>
      WalLogAgg.merge(iter.map(WalLogAgg(_)), aggregateParam)
    }.toDF(WalLogAgg.outputColumns: _*)
  }
}


/**
  * expect DataFrame of WalLog, then group WalLog by groupByKeys(default from).
  * produce DataFrame of WalLogAgg which abstract the session consists of sequence of WalLog ordered by timestamp(desc).
  *
  * one way to visualize this is that transforming (row, column, value) matrix entries into (row, Sparse Vector(column:value).
  * note that we only keep track of max topK latest walLog per each groupByKeys
  */
class WalLogAggregateProcess(taskConf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(taskConf) {

  import WalLogAggregateProcess._

  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    import ss.implicits._

    val groupByKeys = taskConf.options.get("groupByKeys").map(_.split(",").filter(_.nonEmpty).toSeq)
    val maxNumOfEdges = taskConf.options.get("maxNumOfEdges").map(_.toInt).getOrElse(1000)
    val arrayType = taskConf.options.get("arrayType").map(_.toBoolean).getOrElse(false)
    val sortTopItems = taskConf.options.get("sortTopItems").map(_.toBoolean).getOrElse(false)

    taskConf.options.get("parallelism").foreach(d => ss.sqlContext.setConf("spark.sql.shuffle.partitions", d))

    implicit val ord = WalLog.orderByTsAsc
    val walLogs = taskConf.inputs.tail.foldLeft(inputMap(taskConf.inputs.head)) { case (prev, cur) =>
      prev.union(inputMap(cur))
    }

    //TODO: Current implementation only expect taskConf.options as Map[String, String].
    //Once we change taskConf.options as JsObject, then we can simply parse input paramter as following.
    //implicit val paramReads = Json.reads[AggregateParam]

    val param = AggregateParam(groupByKeys, topK = Option(maxNumOfEdges),
      isArrayType = Option(arrayType), shouldSortTopItems = Option(sortTopItems))

    if (arrayType) aggregate(ss, walLogs.as[WalLogAgg], param)
    else aggregateRaw(ss, walLogs.as[WalLog], param)
  }

  override def mandatoryOptions: Set[String] = Set.empty
}
