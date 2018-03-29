package org.apache.s2graph.s2jobs.task

import org.apache.s2graph.s2jobs.Logger

case class TaskConf(name:String, `type`:String, inputs:Seq[String] = Nil, options:Map[String, String] = Map.empty)

trait Task extends Serializable with Logger {
  val conf:TaskConf
  val LOG_PREFIX = s"[${this.getClass.getSimpleName}]"

  def mandatoryOptions:Set[String]
  def isValidate:Boolean = mandatoryOptions.subsetOf(conf.options.keySet)

  require(isValidate, s"""${LOG_PREFIX} not exists mandatory options '${mandatoryOptions.mkString(",")}'
                          in task options (${conf.options.keySet.mkString(",")})
                      """.stripMargin)

  println(s"${LOG_PREFIX} init : $conf")
}
