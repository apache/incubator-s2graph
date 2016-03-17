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

package org.apache.s2graph.counter.core

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.{Graph, Management}
import org.apache.s2graph.counter.config.S2CounterConfig
import org.apache.s2graph.counter.core.TimedQualifier.IntervalUnit
import org.apache.s2graph.counter.core.v2.{GraphOperation, RankingStorageGraph}
import org.apache.s2graph.counter.helper.CounterAdmin
import org.apache.s2graph.counter.models.{Counter, CounterModel, DBModel}
import org.apache.s2graph.counter.util.Retry
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import play.api.libs.json.Json

import scala.util.{Failure, Random, Success, Try}

class RankingCounterSpec extends Specification with BeforeAfterAll {
  val config = ConfigFactory.load()
  DBModel.initialize(config)
  val counterModel = new CounterModel(config)
  val admin = new CounterAdmin(config)

  val s2config = new S2CounterConfig(config)

//  val rankingCounterV1 = new RankingCounter(config, new RankingStorageV1(config))
//  val rankingCounterV2 = new RankingCounter(config, new RankingStorageV2(config))
  val rankingCounterV2 = new RankingCounter(config, new RankingStorageGraph(config))

//  "RankingCounterV1" >> {
//    val policy = counterModel.findByServiceAction("test", "test_action", useCache = false).get
//    val rankingKey = RankingKey(policy.id, policy.version, ExactQualifier(TimedQualifier(IntervalUnit.TOTAL, 0L), Map.empty[String, String]))
//    "get top k" >> {
//      val result = rankingCounterV1.getTopK(rankingKey, 100)
//
//      println(result)
//
//      result must not be empty
//    }
//
//    "get and increment" >> {
//      val result = rankingCounterV1.getTopK(rankingKey, 100).get
//
//      val value = 2d
//      val contents = {
//        for {
//          (item, score) <- result.values
//        } yield {
//          item -> RankingValue(score + value, value)
//        }
//      }.toMap
//      rankingCounterV1.update(rankingKey, contents, 100)
//
//      val result2 = rankingCounterV1.getTopK(rankingKey, 100).get
//
//      result2.totalScore must_== result.totalScore + contents.values.map(_.increment).sum
//      result2.values must containTheSameElementsAs(result.values.map { case (k, v) => (k, v + value) })
//    }
//  }

  val service = "test"
  val action = "test_case"

  override def beforeAll: Unit = {
    Try {
      Retry(3) {
        admin.setupCounterOnGraph
      }

      val graphOp = new GraphOperation(config)
      val graph = new Graph(config)(scala.concurrent.ExecutionContext.global)
      val management = new Management(graph)
      management.createService(service, s2config.HBASE_ZOOKEEPER_QUORUM, s"${service}_dev", 1, None, "gz")
      val strJs =
        s"""
           |{
           |  "label": "$action",
           |  "srcServiceName": "$service",
           |  "srcColumnName": "src",
           |  "srcColumnType": "string",
           |  "tgtServiceName": "$service",
           |  "tgtColumnName": "$action",
           |  "tgtColumnType": "string",
           |  "indices": [
           |  ],
           |  "props": [
           |  ]
           |}
       """.stripMargin
      Retry(3) {
        if (Label.findByName(action).isEmpty) {
          graphOp.createLabel(Json.parse(strJs))
        }
      }

      admin.deleteCounter(service, action).foreach {
        case Failure(ex) =>
          println(s"$ex")
          throw ex
        case Success(v) =>
      }
      admin.createCounter(Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING, autoComb = true, "", useRank = true))
    } match {
      case Failure(ex) =>
        println(s"$ex")
      case Success(_) =>
    }
  }

  override def afterAll: Unit = {
    admin.deleteCounter(service, action)
  }

  "RankingCounterV2" >> {
    "get top k" >> {
      val policy = counterModel.findByServiceAction(service, action, useCache = true).get

      val rankingKey = RankingKey(policy.id, policy.version, ExactQualifier(TimedQualifier(IntervalUnit.TOTAL, 0L), Map.empty[String, String]))

      val orgMap = Map(
        "1" -> 1d,
        "2" -> 2d,
        "3" -> 3d,
        "4" -> 4d,
        "5" -> 5d,
        "6" -> 6d,
        "7" -> 7d,
        "8" -> 8d,
        "9" -> 9d,
        "10" -> 10d,
        "11" -> 11d,
        "12" -> 12d,
        "13" -> 13d,
        "14" -> 14d,
        "15" -> 15d,
        "16" -> 16d,
        "17" -> 17d,
        "18" -> 18d,
        "19" -> 19d,
        "20" -> 20d,
        "100" -> 100d
      )

      val valueMap = Random.shuffle(orgMap).toMap

      val predictResult = valueMap.toSeq.sortBy(-_._2)

      val rvMap = valueMap.map { case (k, score) =>
        k -> RankingValue(score, 0)
      }
      Try {
        rankingCounterV2.update(rankingKey, rvMap, 100)
      }.isSuccess must_== true

      Thread.sleep(1000)

      val result : RankingResult = rankingCounterV2.getTopK(rankingKey, 10).get

      println(result.values)

      result must not be empty
      result.values must have size 10
      result.values must_== predictResult.take(10)
    }
  }
}
