package org.apache.s2graph.s2jobs.udfs

import org.apache.s2graph.s2jobs.Logger
import org.apache.spark.sql.SparkSession

case class UdfOption(name:String, `class`:String, params:Option[Map[String, String]] = None)
trait Udf extends Serializable with Logger {
  def register(ss: SparkSession, name:String, options:Map[String, String])
}





