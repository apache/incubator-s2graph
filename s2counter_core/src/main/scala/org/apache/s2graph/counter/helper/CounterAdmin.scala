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

package org.apache.s2graph.counter.helper

import com.typesafe.config.Config
import org.apache
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.counter
import org.apache.s2graph.counter.config.S2CounterConfig
import org.apache.s2graph.counter.core.{RankingCounter, ExactCounter}
import org.apache.s2graph.counter.core.v1.{RankingStorageRedis, ExactStorageAsyncHBase}
import org.apache.s2graph.counter.core.v2.{RankingStorageGraph, ExactStorageGraph, GraphOperation}
import org.apache.s2graph.counter.models.{Counter, CounterModel}
import play.api.libs.json.Json

import scala.util.Try

class CounterAdmin(config: Config) {
   val s2config = new S2CounterConfig(config)
   val counterModel = new CounterModel(config)
   val graphOp = new GraphOperation(config)
   val s2graph = new S2Graph(config)(scala.concurrent.ExecutionContext.global)
   val storageManagement = new org.apache.s2graph.core.Management(s2graph)

   def setupCounterOnGraph(): Unit = {
     // create s2counter service
     val service = "s2counter"
     storageManagement.createService(service, s2config.HBASE_ZOOKEEPER_QUORUM, s"$service-${config.getString("phase")}", 1, None, "gz")
     // create bucket label
     val label = "s2counter_topK_bucket"
     if (Label.findByName(label, useCache = false).isEmpty) {
       val strJs =
         s"""
            |{
            |  "label": "$label",
            |  "srcServiceName": "s2counter",
            |  "srcColumnName": "dimension",
            |  "srcColumnType": "string",
            |  "tgtServiceName": "s2counter",
            |  "tgtColumnName": "bucket",
            |  "tgtColumnType": "string",
            |  "indices": [
            |    {"name": "time", "propNames": ["time_unit", "date_time"]}
            |	],
            |	"props": [
            |      {"name": "time_unit", "dataType": "string", "defaultValue": ""},
            |      {"name": "date_time", "dataType": "long", "defaultValue": 0}
            |	],
            |  "hTableName": "s2counter_60",
            |  "hTableTTL": 5184000
            |}
         """.stripMargin
       graphOp.createLabel(Json.parse(strJs))
     }
   }

   def createCounter(policy: Counter): Unit = {
     val newPolicy = policy.copy(hbaseTable = Some(makeHTableName(policy)))
     prepareStorage(newPolicy)
     counterModel.createServiceAction(newPolicy)
   }

   def deleteCounter(service: String, action: String): Option[Try[Unit]] = {
     for {
       policy <- counterModel.findByServiceAction(service, action, useCache = false)
     } yield {
       Try {
         exactCounter(policy).destroy(policy)
         if (policy.useRank) {
           rankingCounter(policy).destroy(policy)
         }
         counterModel.deleteServiceAction(policy)
       }
     }
   }

   def prepareStorage(policy: Counter): Unit = {
     if (policy.rateActionId.isEmpty) {
       // if defined rate action, do not use exact counter
       exactCounter(policy).prepare(policy)
     }
     if (policy.useRank) {
       rankingCounter(policy).prepare(policy)
     }
   }

   def prepareStorage(policy: Counter, version: Byte): Unit = {
     // this function to prepare storage by version parameter instead of policy.version
     prepareStorage(policy.copy(version = version))
   }

   private val exactCounterMap = Map(
     counter.VERSION_1 -> new ExactCounter(config, new ExactStorageAsyncHBase(config)),
     counter.VERSION_2 -> new ExactCounter(config, new ExactStorageGraph(config))
   )
   private val rankingCounterMap = Map(
     apache.s2graph.counter.VERSION_1 -> new RankingCounter(config, new RankingStorageRedis(config)),
     apache.s2graph.counter.VERSION_2 -> new RankingCounter(config, new RankingStorageGraph(config))
   )

   private val tablePrefixMap = Map (
     apache.s2graph.counter.VERSION_1 -> "s2counter",
     apache.s2graph.counter.VERSION_2 -> "s2counter_v2"
   )

   def exactCounter(version: Byte): ExactCounter = exactCounterMap(version)
   def exactCounter(policy: Counter): ExactCounter = exactCounter(policy.version)
   def rankingCounter(version: Byte): RankingCounter = rankingCounterMap(version)
   def rankingCounter(policy: Counter): RankingCounter = rankingCounter(policy.version)

   def makeHTableName(policy: Counter): String = {
     Seq(tablePrefixMap(policy.version), policy.service, policy.ttl) ++ policy.dailyTtl mkString "_"
   }
 }
