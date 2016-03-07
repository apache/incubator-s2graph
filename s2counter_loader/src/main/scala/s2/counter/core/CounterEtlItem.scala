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

import org.slf4j.LoggerFactory
import play.api.libs.json._
import s2.util.UnitConverter

import scala.util.{Failure, Success, Try}

/**
*  Created by hsleep(honeysleep@gmail.com) on 15. 10. 6..
*/
case class CounterEtlItem(ts: Long, service: String, action: String, item: String, dimension: JsValue, property: JsValue, useProfile: Boolean = false) {
  def toKafkaMessage: String = {
    s"$ts\t$service\t$action\t$item\t${dimension.toString()}\t${property.toString()}"
  }

  lazy val value = {
    property \ "value" match {
      case JsNumber(n) => n.longValue()
      case JsString(s) => s.toLong
      case _: JsUndefined => 1L
      case _ => throw new Exception("wrong type")
    }
  }
}

object CounterEtlItem {
  val log = LoggerFactory.getLogger(this.getClass)

  def apply(line: String): Option[CounterEtlItem] = {
    Try {
      val Array(ts, service, action, item, dimension, property) = line.split('\t')
      CounterEtlItem(UnitConverter.toMillis(ts.toLong), service, action, item, Json.parse(dimension), Json.parse(property))
    } match {
      case Success(item) =>
        Some(item)
      case Failure(ex) =>
        log.error(">>> failed")
        log.error(s"${ex.toString}: $line")
        None
    }
  }
}
