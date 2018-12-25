package org.apache.s2graph.s2jobs.wal.utils

import org.apache.s2graph.core.S2GraphConfigs

object TaskConfUtil {

  def parseHBaseConfigs(options: Map[String, String]): Map[String, Any] = {
    options.filterKeys(S2GraphConfigs.HBaseConfigs.DEFAULTS.keySet)
  }

  def parseMetaStoreConfigs(options: Map[String, String]): Map[String, Any] = {
    options.filterKeys(S2GraphConfigs.DBConfigs.DEFAULTS.keySet)
  }

  def parseLocalCacheConfigs(options: Map[String, String]): Map[String, Any] = {
    options.filterKeys(S2GraphConfigs.CacheConfigs.DEFAULTS.keySet).mapValues(_.toInt)
  }
}
