package org.apache.s2graph.s2jobs.task

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Process
  * @param conf
  */
abstract class Process(override val conf:TaskConf) extends Task {
  def execute(ss:SparkSession, inputMap:Map[String, DataFrame]):DataFrame
}

/**
  * SqlProcess
  * @param conf
  */
class SqlProcess(conf:TaskConf) extends Process(conf) {
  override def mandatoryOptions: Set[String] = Set("sql")

  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    // create temp table
    inputMap.foreach { case (name, df) =>
      logger.debug(s"${LOG_PREFIX} create temp table : $name")
      df.printSchema()
      df.createOrReplaceTempView(name)
    }

    val sql = conf.options("sql")
    logger.debug(s"${LOG_PREFIX} sql : $sql")

    ss.sql(sql)
  }
}

