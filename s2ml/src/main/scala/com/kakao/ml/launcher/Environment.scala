package com.kakao.ml.launcher

object Environment {

  var jobId = "not defined"

  var env = Map.empty[String, Any]

  val batchId: String = System.currentTimeMillis().toString

}

trait Environment {

  def setEnv(env: Map[String, Any]): Unit = Environment.env = env

  def setJobId(jobId: String): Unit = Environment.jobId = jobId

  def getJobId: String = Environment.jobId

  def getBatchId: String = Environment.batchId

  def getBatchDir(prefix: String, suffix: String): String = s"$prefix/$getJobId/$getBatchId/$suffix"

  def getJobDir(prefix: String): String = s"$prefix/$getJobId"

}
