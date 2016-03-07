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

import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 26..
 */
class CounterModelSpec extends Specification {
  val config = ConfigFactory.load()

  DBModel.initialize(config)

  "CounterModel" should {
    val model = new CounterModel(config)
    "findById" in {
      model.findById(0, useCache = false) must beNone
    }

    "findByServiceAction using cache" in {
      val service = "test"
      val action = "test_action"
      val counter = Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING,
        autoComb = true, "", useProfile = true, None, useRank = true, 0, None, None, None, None, None, None)
      model.createServiceAction(counter)
      model.findByServiceAction(service, action, useCache = false) must beSome
      val opt = model.findByServiceAction(service, action, useCache = true)
      opt must beSome
      model.findById(opt.get.id) must beSome
      model.deleteServiceAction(opt.get)
      model.findById(opt.get.id) must beSome
      model.findById(opt.get.id, useCache = false) must beNone
    }

    "create and delete policy" in {
      val (service, action) = ("test", "test_case")
      for {
        policy <- model.findByServiceAction(service, action, useCache = false)
      } {
        model.deleteServiceAction(policy)
      }
      model.createServiceAction(Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING,
        autoComb = true, "", useProfile = true, None, useRank = true, 0, None, None, None, None, None, None))
      model.findByServiceAction(service, action, useCache = false).map { policy =>
        policy.service mustEqual service
        policy.action mustEqual action
        model.deleteServiceAction(policy)
        policy
      } must beSome
      model.findByServiceAction(service, action, useCache = false) must beNone
    }
  }
}
