package org.apache.s2graph.spark.sql.streaming

object S2SourceConfigs {
  val S2_SOURCE_PREFIX = "s2.spark.sql.streaming.source"
  val S2_SOURCE_BULKLOAD_PREFIX = "s2.spark.sql.bulkload.source"

  //
  // vertex/indexedge/snapshotedge
  val S2_SOURCE_ELEMENT_TYPE = s"$S2_SOURCE_PREFIX.element.type"

  // HBASE HFILE BULK
  val S2_SOURCE_BULKLOAD_HBASE_ROOT_DIR = s"$S2_SOURCE_BULKLOAD_PREFIX.hbase.rootdir"
  val S2_SOURCE_BULKLOAD_RESTORE_PATH = s"$S2_SOURCE_BULKLOAD_PREFIX.restore.path"
  val S2_SOURCE_BULKLOAD_HBASE_TABLE_NAMES = s"$S2_SOURCE_BULKLOAD_PREFIX.hbase.table.names"
  val S2_SOURCE_BULKLOAD_HBASE_TABLE_CF = s"$S2_SOURCE_BULKLOAD_PREFIX.hbase.table.cf"
  val S2_SOURCE_BULKLOAD_SCAN_BATCH_SIZE = s"$S2_SOURCE_BULKLOAD_PREFIX.scan.batch.size"

  // BULKLOAD
  val S2_SOURCE_BULKLOAD_BUILD_DEGREE = s"$S2_SOURCE_BULKLOAD_PREFIX.build.degree"

}
