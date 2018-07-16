package org.apache.s2graph.s2jobs.wal.process

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.udafs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * expect S2EdgeData dataframe as input.
  * @param taskConf
  */
class S2EdgeDataAggregateProcess(taskConf: TaskConf) extends org.apache.s2graph.s2jobs.task.Process(taskConf) {
  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    val maxNumOfEdges = taskConf.options.get("maxNumOfEdges").map(_.toInt).getOrElse(1000)
    val groupByColumns = taskConf.options.get("groupByColumns").getOrElse("from").split(",").map(col(_))
    val aggregateColumns = taskConf.options.get("aggregateColumns").getOrElse("timestamp,to,label,props").split(",").map(col(_))
    taskConf.options.get("parallelism").map(ss.sqlContext.setConf("spark.sql.shuffle.partitions", _))

    val aggregator = S2EdgeDataAggregate(maxNumOfEdges)

    val edges = inputMap(taskConf.inputs.head)

    edges
      .groupBy(groupByColumns: _*)
      .agg(
        aggregator(aggregateColumns: _*).as("edges"),
        max(col("timestamp")).as("max_ts"),
        min(col("timestamp")).as("min_ts")
      )
  }

  override def mandatoryOptions: Set[String] = Set.empty
}
