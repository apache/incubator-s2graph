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

package org.apache.s2graph.core.storage

import scala.concurrent.{ExecutionContext, Future}

trait StorageWritable {
  /**
    * decide how to store given key values Seq[SKeyValue] into storage using storage's client.
    * note that this should be return true on all success.
    * we assumes that each storage implementation has client as member variable.
    *
    *
    * @param cluster: where this key values should be stored.
    * @param kvs: sequence of SKeyValue that need to be stored in storage.
    * @param withWait: flag to control wait ack from storage.
    *                  note that in AsynchbaseStorage(which support asynchronous operations), even with true,
    *                  it never block thread, but rather submit work and notified by event loop when storage send ack back.
    * @return ack message from storage.
    */
  def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse]

  /**
    * write requestKeyValue into storage if the current value in storage that is stored matches.
    * note that we only use SnapshotEdge as place for lock, so this method only change SnapshotEdge.
    *
    * Most important thing is this have to be 'atomic' operation.
    * When this operation is mutating requestKeyValue's snapshotEdge, then other thread need to be
    * either blocked or failed on write-write conflict case.
    *
    * Also while this method is still running, then fetchSnapshotEdgeKeyValues should be synchronized to
    * prevent wrong data for read.
    *
    * Best is use storage's concurrency control(either pessimistic or optimistic) such as transaction,
    * compareAndSet to synchronize.
    *
    * for example, AsynchbaseStorage use HBase's CheckAndSet atomic operation to guarantee 'atomicity'.
    * for storage that does not support concurrency control, then storage implementation
    * itself can maintain manual locks that synchronize read(fetchSnapshotEdgeKeyValues)
    * and write(writeLock).
    * @param requestKeyValue
    * @param expectedOpt
    * @return
    */
  def writeLock(requestKeyValue: SKeyValue, expectedOpt: Option[SKeyValue])(implicit ec: ExecutionContext): Future[MutateResponse]

}
