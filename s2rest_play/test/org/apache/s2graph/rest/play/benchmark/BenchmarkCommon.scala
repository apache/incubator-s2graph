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

package org.apache.s2graph.rest.play.benchmark

import org.specs2.mutable.Specification

trait BenchmarkCommon extends Specification {
  val wrapStr = s"\n=================================================="

  def duration[T](prefix: String = "")(block: => T) = {
    val startTs = System.currentTimeMillis()
    val ret = block
    val endTs = System.currentTimeMillis()
    println(s"$wrapStr\n$prefix: took ${endTs - startTs} ms$wrapStr")
    ret
  }

  def durationWithReturn[T](prefix: String = "")(block: => T): (T, Long) = {
    val startTs = System.currentTimeMillis()
    val ret = block
    val endTs = System.currentTimeMillis()
    val duration = endTs - startTs
//    println(s"$wrapStr\n$prefix: took $duration ms$wrapStr")
    (ret, duration)
  }
}
