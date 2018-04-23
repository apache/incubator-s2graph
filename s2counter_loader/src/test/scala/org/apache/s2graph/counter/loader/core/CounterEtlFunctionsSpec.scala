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

package org.apache.s2graph.counter.loader.core

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.mysqls.{Label, Service}
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.{S2Graph, Management}
import org.apache.s2graph.counter.models.DBModel
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class CounterEtlFunctionsSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
  val config = ConfigFactory.load()
  val cluster = config.getString("hbase.zookeeper.quorum")
  DBModel.initialize(config)

  val graph = new S2Graph(config)(global)
  val management = new Management(graph)

  override def beforeAll: Unit = {
    management.createService("test", cluster, "test", 1, None, "gz")
    management.createLabel("test_case", "test", "src", "string", "test", "tgt", "string", true, "test", Nil, Nil, "weak", None, None, HBaseType.DEFAULT_VERSION, false, "gz")
  }

  override def afterAll: Unit = {
    Label.delete(Label.findByName("test_case", false).get.id.get)
    Service.delete(Service.findByName("test", false).get.id.get)
  }

  "CounterEtlFunctions" should "parsing log" in {
    val data =
      """
        |1435107139287	insert	e	aaPHfITGUU0B_150212123559509	abcd	test_case	{"cateid":"100110102","shopid":"1","brandid":""}
        |1435106916136	insert	e	Tgc00-wtjp2B_140918153515441	efgh	test_case	{"cateid":"101104107","shopid":"2","brandid":""}
      """.stripMargin.trim.split('\n')
    val items = {
      for {
        line <- data
        item <- CounterEtlFunctions.parseEdgeFormat(line)
      } yield {
        item.action should equal("test_case")
        item
      }
    }

    items should have size 2
  }
}
