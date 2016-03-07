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

package s2.counter.core

import com.kakao.s2graph.core.{Edge, Graph, GraphUtil}
import org.apache.spark.Logging
import play.api.libs.json._
import s2.config.{S2ConfigFactory, StreamingConfig}
import s2.models.CounterModel

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 3. 17..
 */
object CounterEtlFunctions extends Logging {
  lazy val filterOps = Seq("insert", "insertBulk", "update", "increment").map(op => GraphUtil.operations(op))
  lazy val preFetchSize = StreamingConfig.PROFILE_PREFETCH_SIZE
  lazy val config = S2ConfigFactory.config
  lazy val counterModel = new CounterModel(config)

  def logToEdge(line: String): Option[Edge] = {
    for {
      elem <- Graph.toGraphElement(line) if elem.isInstanceOf[Edge]
      edge <- Some(elem.asInstanceOf[Edge]).filter { x =>
        filterOps.contains(x.op)
      }
    } yield {
      edge
    }
  }

  def parseEdgeFormat(line: String): Option[CounterEtlItem] = {
    /**
     * 1427082276804	insert	edge	19073318	52453027_93524145648511699	story_user_ch_doc_view	{"doc_type" : "l", "channel_subscribing" : "y", "view_from" : "feed"}
     */
    for {
      elem <- Graph.toGraphElement(line) if elem.isInstanceOf[Edge]
      edge <- Some(elem.asInstanceOf[Edge]).filter { x =>
        filterOps.contains(x.op)
      }
    } yield {
      val label = edge.label
      val labelName = label.label
      val tgtService = label.tgtColumn.service.serviceName
      val tgtId = edge.tgtVertex.innerId.toString()
      val srcId = edge.srcVertex.innerId.toString()

      // make empty property if no exist edge property
      val dimension = Json.parse(Some(GraphUtil.split(line)).filter(_.length >= 7).map(_(6)).getOrElse("{}"))
      val bucketKeys = Seq("_from")
      val bucketKeyValues = {
        for {
          variable <- bucketKeys
        } yield {
          val jsValue = variable match {
            case "_from" => JsString(srcId)
            case s => dimension \ s
          }
          s"[[$variable]]" -> jsValue
        }
      }
      val property = Json.toJson(bucketKeyValues :+ ("value" -> JsString("1")) toMap)
//      val property = Json.toJson(Map("_from" -> srcId, "_to" -> tgtId, "value" -> "1"))

      CounterEtlItem(edge.ts, tgtService, labelName, tgtId, dimension, property)
    }
  }

  def parseEdgeFormat(lines: List[String]): List[CounterEtlItem] = {
    for {
      line <- lines
      item <- parseEdgeFormat(line)
    } yield {
      item
    }
  }
  
  def checkPolicyAndMergeDimension(service: String, action: String, items: List[CounterEtlItem]): List[CounterEtlItem] = {
    counterModel.findByServiceAction(service, action).map { policy =>
      if (policy.useProfile) {
        policy.bucketImpId match {
          case Some(_) => DimensionProps.mergeDimension(policy, items)
          case None => Nil
        }
      } else {
        items
      }
    }.getOrElse(Nil)
  }
}
