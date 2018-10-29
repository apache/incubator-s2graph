package org.apache.s2graph.s2jobs.wal.process

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.process.params.AggregateParam
import org.apache.s2graph.s2jobs.wal.{WalLog, WalLogAgg}
import org.apache.spark.sql._

object WalLogAggregateProcess {
  def aggregate(ss: SparkSession,
                dataset: Dataset[WalLogAgg],
                aggregateParam: AggregateParam)(implicit ord: Ordering[WalLog]) = {
    import ss.implicits._
    dataset.groupByKey(_.from).flatMapGroups { case (_, iter) =>
      WalLogAgg.merge(iter, aggregateParam)
    }.toDF(WalLogAgg.outputColumns: _*)
  }

  def aggregateRaw(ss: SparkSession,
                   dataset: Dataset[WalLog],
                   aggregateParam: AggregateParam)(implicit ord: Ordering[WalLog]): DataFrame = {
    import ss.implicits._

    dataset.groupByKey(walLog => walLog.from).flatMapGroups { case (key, iter) =>
      WalLogAgg.mergeWalLogs(iter, aggregateParam)
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

    //TODO: Current implementation only expect taskConf.options as Map[String, String].
    //Once we change taskConf.options as JsObject, then we can simply parse input paramter as following.
    //implicit val paramReads = Json.reads[AggregateParam]
    val param = AggregateParam.fromTaskConf(taskConf)
    param.numOfPartitions.foreach(d => ss.sqlContext.setConf("spark.sql.shuffle.partitions", d.toString))

    implicit val ord = WalLog.orderByTsAsc
    val walLogs = taskConf.inputs.tail.foldLeft(inputMap(taskConf.inputs.head)) { case (prev, cur) =>
      prev.union(inputMap(cur))
    }

    if (param.arrayType) aggregate(ss, walLogs.as[WalLogAgg], param)
    else aggregateRaw(ss, walLogs.as[WalLog], param)
  }

  override def mandatoryOptions: Set[String] = Set.empty
}
