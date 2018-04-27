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

package org.apache.s2graph.core

import java.util.function.BiConsumer

import org.apache.s2graph.core.S2Edge.Props
import org.apache.s2graph.core.schema.LabelMeta
import org.apache.s2graph.core.types.{InnerVal, InnerValLikeWithTs}
import org.apache.tinkerpop.gremlin.structure.Property

object S2EdgePropertyHelper {
  def propertyInner[V](edge: S2EdgeLike, key: String, value: V, ts: Long): Property[V] = {
    val labelMeta = edge.innerLabel.metaPropsInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Edge."))
    val newProp = new S2Property[V](edge, labelMeta, key, value, ts)
    edge.getPropsWithTs().put(key, newProp)
    newProp
  }

  def updatePropsWithTs(edge: S2EdgeLike, others: Props = S2Edge.EmptyProps): Props = {
    val emptyProp = S2Edge.EmptyProps

    edge.getPropsWithTs().forEach(new BiConsumer[String, S2Property[_]] {
      override def accept(key: String, value: S2Property[_]): Unit = emptyProp.put(key, value)
    })

    others.forEach(new BiConsumer[String, S2Property[_]] {
      override def accept(key: String, value: S2Property[_]): Unit = emptyProp.put(key, value)
    })

    emptyProp
  }

  def propertyValue(e: S2EdgeLike, key: String): Option[InnerValLikeWithTs] = {
    key match {
      case "from" | "_from" => Option(InnerValLikeWithTs(e.srcVertex.innerId, e.ts))
      case "to" | "_to" => Option(InnerValLikeWithTs(e.tgtVertex.innerId, e.ts))
      case "label" => Option(InnerValLikeWithTs(InnerVal.withStr(e.innerLabel.label, e.innerLabel.schemaVersion), e.ts))
      case "direction" => Option(InnerValLikeWithTs(InnerVal.withStr(e.getDirection(), e.innerLabel.schemaVersion), e.ts))
      case _ =>
        e.innerLabel.metaPropsInvMap.get(key).map(labelMeta => e.propertyValueInner(labelMeta))
    }
  }

  def propertyValuesInner(edge: S2EdgeLike, labelMetas: Seq[LabelMeta] = Nil): Map[LabelMeta, InnerValLikeWithTs] = {
    if (labelMetas.isEmpty) {
      edge.innerLabel.metaPropsDefaultMapInner.map { case (labelMeta, defaultVal) =>
        labelMeta -> edge.propertyValueInner(labelMeta)
      }
    } else {
      // This is important since timestamp is required for all edges.
      (LabelMeta.timestamp +: labelMetas).map { labelMeta =>
        labelMeta -> propertyValueInner(edge, labelMeta)
      }.toMap
    }
  }

  def propertyValueInner(edge: S2EdgeLike, labelMeta: LabelMeta): InnerValLikeWithTs = {
    //    propsWithTs.get(labelMeta.name).map(_.innerValWithTs).getOrElse()
    if (edge.getPropsWithTs().containsKey(labelMeta.name)) {
      edge.getPropsWithTs().get(labelMeta.name).innerValWithTs
    } else {
      edge.innerLabel.metaPropsDefaultMapInner(labelMeta)
    }
  }


  def toLabelMetas(edge: S2EdgeLike, keys: Seq[String]): Seq[LabelMeta] = {
    for {
      key <- keys
      labelMeta <- edge.innerLabel.metaPropsInvMap.get(key)
    } yield labelMeta
  }

}
