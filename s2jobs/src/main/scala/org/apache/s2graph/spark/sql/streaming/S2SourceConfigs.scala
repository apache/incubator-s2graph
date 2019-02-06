/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
  val S2_SOURCE_BULKLOAD_LABEL_NAMES = s"$S2_SOURCE_BULKLOAD_PREFIX.label.names"

  // BULKLOAD
  val S2_SOURCE_BULKLOAD_BUILD_DEGREE = s"$S2_SOURCE_BULKLOAD_PREFIX.build.degree"

}
