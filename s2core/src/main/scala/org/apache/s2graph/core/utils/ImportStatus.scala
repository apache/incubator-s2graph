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

package org.apache.s2graph.core.utils

import java.util.concurrent.atomic.AtomicInteger

trait ImportStatus {
  val done: AtomicInteger

  def isCompleted: Boolean

  def percentage: Int

  val total: Int
}

class ImportRunningStatus(val total: Int) extends ImportStatus {
  require(total > 0, s"Total should be positive: $total")

  val done = new AtomicInteger(0)

  def isCompleted: Boolean = total == done.get

  def percentage = 100 * done.get / total
}

case object ImportDoneStatus extends ImportStatus {
  val total = 1

  val done = new AtomicInteger(1)

  def isCompleted: Boolean = true

  def percentage = 100
}

object ImportStatus {
  def apply(total: Int): ImportStatus = new ImportRunningStatus(total)

  def unapply(importResult: ImportStatus): Option[(Boolean, Int, Int)] =
    Some((importResult.isCompleted, importResult.total, importResult.done.get))
}
