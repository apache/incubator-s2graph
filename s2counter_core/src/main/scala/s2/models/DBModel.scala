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

package s2.models

import com.typesafe.config.Config
import s2.config.S2CounterConfig
import scalikejdbc._

/**
 * Created by alec on 15. 3. 31..
 */
object DBModel {
  private var initialized = false
  
  def initialize(config: Config): Unit = {
    if (!initialized) {
      this synchronized {
        if (!initialized) {
          val s2Config = new S2CounterConfig(config)
          Class.forName(s2Config.DB_DEFAULT_DRIVER)
          val settings = ConnectionPoolSettings(initialSize = 0, maxSize = 10, connectionTimeoutMillis = 5000L, validationQuery = "select 1;")

          ConnectionPool.singleton(s2Config.DB_DEFAULT_URL, s2Config.DB_DEFAULT_USER, s2Config.DB_DEFAULT_PASSWORD, settings)
          initialized = true
        }
      }
    }
  }
}
