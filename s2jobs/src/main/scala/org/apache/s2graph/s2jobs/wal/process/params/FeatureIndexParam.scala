package org.apache.s2graph.s2jobs.wal.process.params

object FeatureIndexParam {
  val defaultMinUserCount = 0L
  val defaultCountColumnName = "from"
}

case class FeatureIndexParam(minUserCount: Option[Long],
                             countColumnName: Option[String],
                             samplePointsPerPartitionHint: Option[Int],
                             numOfPartitions: Option[Int],
                             maxRankPerDim: Option[Map[String, Int]],
                             defaultMaxRank: Option[Int],
                             dictPath: Option[String]) {
  import FeatureIndexParam._
  val _countColumnName = countColumnName.getOrElse(defaultCountColumnName)
  val _minUserCount = minUserCount.getOrElse(defaultMinUserCount)
  val _maxRankPerDim = maxRankPerDim.getOrElse(Map.empty)
  val _defaultMaxRank = defaultMaxRank.getOrElse(Int.MaxValue)
}
