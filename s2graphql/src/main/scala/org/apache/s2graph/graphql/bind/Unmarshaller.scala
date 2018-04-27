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

package org.apache.s2graph.graphql.bind

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.core.schema.ServiceColumn
import org.apache.s2graph.core.{S2EdgeLike, S2VertexLike}
import org.apache.s2graph.graphql.repository.GraphRepository
import org.apache.s2graph.graphql.types.S2Type._
import sangria.marshalling._
import sangria.schema.Context

object Unmarshaller {
  type RawNode = Map[String, Any]

  def unwrap(any: Any): Any = any match {
    case s: Some[_] => unwrap(s.get)
    case v: Seq[_] => v.map(unwrap)
    case m: Map[_, _] => m.mapValues(unwrap)
    case _ => any
  }

  implicit object AddVertexParamFromInput extends FromInput[List[AddVertexParam]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val now = System.currentTimeMillis()
      val map = unwrap(node).asInstanceOf[RawNode]

      val params = map.flatMap { case (columnName, vls: Vector[_]) =>
        vls.map { _m =>
          val m = _m.asInstanceOf[RawNode]
          val id = m("id")
          val ts = m.getOrElse("timestamp", now).asInstanceOf[Long]

          AddVertexParam(ts, id, columnName, props = m)
        }
      }

      params.toList
    }
  }

  implicit object AddEdgeParamFromInput extends FromInput[AddEdgeParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = unwrap(node).asInstanceOf[RawNode]
      val now = System.currentTimeMillis()

      val from = inputMap("from")
      val to = inputMap("to")
      val ts = inputMap.get("timestamp").map(_.asInstanceOf[Long]).getOrElse(now)
      val dir = inputMap.get("direction").map(_.asInstanceOf[String]).getOrElse("out")
      val props = inputMap

      AddEdgeParam(ts, from, to, dir, props)
    }
  }

  implicit object IndexFromInput extends FromInput[Index] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[RawNode]
      Index(input("name").asInstanceOf[String], input("propNames").asInstanceOf[Seq[String]])
    }
  }

  implicit object PropFromInput extends FromInput[Prop] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[RawNode]

      val name = input("name").asInstanceOf[String]
      val defaultValue = input("defaultValue").asInstanceOf[String]
      val dataType = input("dataType").asInstanceOf[String]
      val storeInGlobalIndex = input("storeInGlobalIndex").asInstanceOf[Boolean]

      Prop(name, defaultValue, dataType, storeInGlobalIndex)
    }
  }

  implicit object ServiceColumnParamFromInput extends FromInput[ServiceColumnParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = ServiceColumnParamsFromInput.fromResult(node).head
  }

  implicit object ServiceColumnParamsFromInput extends FromInput[Vector[ServiceColumnParam]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = unwrap(node.asInstanceOf[Map[String, Any]]).asInstanceOf[Map[String, Any]]

      val partialServiceColumns = input.map { case (serviceName, serviceColumnMap) =>
        val innerMap = serviceColumnMap.asInstanceOf[Map[String, Any]]
        val columnName = innerMap("columnName").asInstanceOf[String]
        val props = innerMap.get("props").toSeq.flatMap { case vs: Vector[_] =>
          vs.map(PropFromInput.fromResult)
        }

        ServiceColumnParam(serviceName, columnName, props)
      }

      partialServiceColumns.toVector
    }
  }
}
