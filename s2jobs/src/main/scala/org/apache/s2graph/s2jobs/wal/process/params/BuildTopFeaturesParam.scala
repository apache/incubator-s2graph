package org.apache.s2graph.s2jobs.wal.process.params

object BuildTopFeaturesParam {
  val defaultMinUserCount = 0L
  val defaultCountColumnName = "from"
}

case class BuildTopFeaturesParam(minUserCount: Option[Long],
                                 countColumnName: Option[String],
                                 samplePointsPerPartitionHint: Option[Int],
                                 numOfPartitions: Option[Int]) {

  import BuildTopFeaturesParam._

  val _countColumnName = countColumnName.getOrElse(defaultCountColumnName)
  val _minUserCount = minUserCount.getOrElse(defaultMinUserCount)
}
