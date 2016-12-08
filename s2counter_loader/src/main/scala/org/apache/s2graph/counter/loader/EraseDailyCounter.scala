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

package org.apache.s2graph.counter.loader

import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.ExecutionContext

import kafka.producer.KeyedMessage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.Json

import org.apache.s2graph.counter._
import org.apache.s2graph.counter.core._
import org.apache.s2graph.counter.core.ExactCounter.ExactValueMap
import org.apache.s2graph.counter.core.v1.ExactStorageHBase
import org.apache.s2graph.counter.core.v2.ExactStorageGraph
import org.apache.s2graph.counter.loader.config.StreamingConfig
import org.apache.s2graph.counter.loader.core.CounterEtlItem
import org.apache.s2graph.counter.models.{Counter, CounterModel, DBModel}
import org.apache.s2graph.spark.config.S2ConfigFactory
import org.apache.s2graph.spark.spark.{SparkApp, WithKafka}

object EraseDailyCounter extends SparkApp with WithKafka {
  implicit val ec = ExecutionContext.Implicits.global

  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  def valueToEtlItem(policy: Counter,
                     key: ExactKeyTrait,
                     values: ExactValueMap): Seq[CounterEtlItem] =
    if (values.nonEmpty) {
      for {
        (eq, value) <- filter(values.toList)
      } yield {
        CounterEtlItem(
          eq.tq.ts,
          policy.service,
          policy.action,
          key.itemKey,
          Json.toJson(eq.dimKeyValues),
          Json.toJson(Map("value" -> -value))
        )
      }
    } else {
      Nil
    }

  def filter(values: List[(ExactQualifier, Long)]): List[(ExactQualifier, Long)] = {
    val sorted = values.sortBy(_._1.dimKeyValues.size).reverse
    val (eq, value) = sorted.head
    val dimKeys = eq.dimKeyValues.toSeq
    val flat = {
      for {
        i <- 0 to dimKeys.length
        comb <- dimKeys.combinations(i)
      } yield {
        ExactQualifier(eq.tq, comb.toMap) -> value
      }
    }.toMap

    val valuesMap = values.toMap
    val remain = (valuesMap ++ flat.map {
      case (k, v) =>
        k -> (valuesMap(k) - v)
    }).filter(_._2 > 0).toList

    if (remain.isEmpty) {
      List((eq, value))
    } else {
      (eq, value) :: filter(remain)
    }
  }

  def produce(policy: Counter, exactRdd: RDD[(ExactKeyTrait, ExactValueMap)]): Unit =
    exactRdd
      .mapPartitions { part =>
        for {
          (key, values) <- part
          item <- valueToEtlItem(policy, key, values)
        } yield {
          item
        }
      }
      .foreachPartition { part =>
        val m = MutableHashMap.empty[Int, mutable.MutableList[CounterEtlItem]]
        part.foreach { item =>
          val k = getPartKey(item.item, 20)
          val values = m.getOrElse(k, mutable.MutableList.empty[CounterEtlItem])
          values += item
          m.update(k, values)
        }
        m.foreach {
          case (k, v) =>
            v.map(_.toKafkaMessage).grouped(1000).foreach { grouped =>
              producer.send(
                new KeyedMessage[String, String](
                  StreamingConfig.KAFKA_TOPIC_COUNTER,
                  null,
                  k,
                  grouped.mkString("\n")
                )
              )
            }
        }
      }

  def rddToExactRdd(policy: Counter,
                    date: String,
                    rdd: RDD[String]): RDD[(ExactKeyTrait, ExactValueMap)] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val fromTs = dateFormat.parse(date).getTime
    val toTs = fromTs + 23 * 60 * 60 * 1000

    rdd.mapPartitions { part =>
      val exactCounter = policy.version match {
        case VERSION_1 =>
          new ExactCounter(S2ConfigFactory.config, new ExactStorageHBase(S2ConfigFactory.config))
        case VERSION_2 =>
          new ExactCounter(S2ConfigFactory.config, new ExactStorageGraph(S2ConfigFactory.config))
      }

      for {
        line <- part
        FetchedCounts(exactKey, qualifierWithCountMap) <- exactCounter
          .getCount(policy, line, Array(TimedQualifier.IntervalUnit.DAILY), fromTs, toTs)
      } yield {
        (exactKey, qualifierWithCountMap)
      }
    }
  }

  lazy val className = getClass.getName.stripSuffix("$")

  override def run(): Unit = {
    validateArgument("service", "action", "date", "file", "op")
    DBModel.initialize(S2ConfigFactory.config)

    val (service, action, date, file, op) = (args(0), args(1), args(2), args(3), args(4))
    val conf = sparkConf(s"$className: $service.$action")

    val ctx = new SparkContext(conf)

    val rdd = ctx.textFile(file, 20)

    val counterModel = new CounterModel(S2ConfigFactory.config)

    val policy = counterModel.findByServiceAction(service, action).get
    val exactRdd = rddToExactRdd(policy, date, rdd)
    produce(policy, exactRdd)
  }
}
