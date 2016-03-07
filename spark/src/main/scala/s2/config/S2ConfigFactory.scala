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

package s2.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created by alec on 15. 3. 4..
 */

/**
 * phase에 따라 phase.conf 파일을 load 해주는 config factory
 */
object S2ConfigFactory {
  lazy val config: Config = _load

  @deprecated("do not call explicitly. use config", "0.0.6")
  def load(): Config = {
    _load
  }

  def _load: Config = {
    // default configuration file name : application.conf
    val sysConfig = ConfigFactory.parseProperties(System.getProperties)

    lazy val phase = if (!sysConfig.hasPath("phase")) "alpha" else sysConfig.getString("phase")
    sysConfig.withFallback(ConfigFactory.parseResourcesAnySyntax(s"$phase.conf")).withFallback(ConfigFactory.load())
  }
}
