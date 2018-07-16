package org.apache.s2graph.s2jobs.wal.process

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.udafs.GroupByArrayAgg
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class S2EdgeDataArrayAggregateProcess(taskConf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(taskConf) {
  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    import ss.sqlContext.implicits._
    val maxNumOfEdges = taskConf.options.get("maxNumOfEdges").map(_.toInt).getOrElse(1000)
    val groupByColumns = taskConf.options.get("groupByColumns").getOrElse("from").split(",").map(col(_))
    val aggregateColumns = taskConf.options.get("aggregateColumns").getOrElse("edges").split(",").map(col(_))
    taskConf.options.get("parallelism").map(ss.sqlContext.setConf("spark.sql.shuffle.partitions", _))
    val aggregator = new GroupByArrayAgg(maxNumOfEdges)

    val edges = inputMap(taskConf.inputs.head)

    edges
      .groupBy(groupByColumns: _*)
      .agg(
        aggregator(aggregateColumns: _*).as("edges"),
        max(col("max_ts")).as("max_ts"),
        min(col("min_ts")).as("min_ts")
      )
  }

  override def mandatoryOptions: Set[String] = Set.empty
}
