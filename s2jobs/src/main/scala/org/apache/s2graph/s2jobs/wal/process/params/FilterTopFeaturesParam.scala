package org.apache.s2graph.s2jobs.wal.process.params

class FilterTopFeaturesParam(maxRankPerDim: Option[Map[String, Int]],
                             defaultMaxRank: Option[Int]) {
  val _maxRankPerDim = maxRankPerDim.getOrElse(Map.empty)
  val _defaultMaxRank = defaultMaxRank.getOrElse(Int.MaxValue)
}
