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

import com.typesafe.config.Config
import org.apache.s2graph.core.S2Graph

import scala.concurrent.ExecutionContext

@deprecated
class S2SinkContext(config: Config)(implicit ec: ExecutionContext = ExecutionContext.Implicits.global){
  println(s">>>> S2SinkContext Created...")
  private lazy val s2Graph = new S2Graph(config)
  def getGraph: S2Graph = {
    s2Graph
  }
}

@deprecated
object S2SinkContext {
  private var s2SinkContext:S2SinkContext = null

  def apply(config:Config):S2SinkContext = {
    if (s2SinkContext == null) {
      s2SinkContext = new S2SinkContext(config)
    }
    s2SinkContext
  }
}
