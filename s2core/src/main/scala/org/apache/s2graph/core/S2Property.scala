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


import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{CanInnerValLike, InnerValLikeWithTs}
import org.apache.tinkerpop.gremlin.structure.Graph.Features
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper
import org.apache.tinkerpop.gremlin.structure._

import scala.util.hashing.MurmurHash3

object S2Property {
  def kvsToProps(kvs: Seq[AnyRef]): Map[String, AnyRef] = {
    import scala.collection.JavaConverters._

    ElementHelper.legalPropertyKeyValueArray(kvs: _*)
    val keySet = collection.mutable.Set[Any]()
    val kvsList = ElementHelper.asPairs(kvs: _*).asScala
    var result = Map[String, AnyRef]()
    kvsList.foreach { pair =>
      val key = pair.getValue0
      val value = pair.getValue1
      ElementHelper.validateProperty(key, value)
      if (keySet.contains(key)) throw VertexProperty.Exceptions.multiPropertiesNotSupported

      assertValidProp(key, value)

      keySet.add(key)
      result = result + (key -> value)
    }

    result
  }

  def assertValidProp[A](key: Any, value: A): Unit = {
    if (key == null) throw Property.Exceptions.propertyKeyCanNotBeEmpty()
    if (!key.isInstanceOf[String]) throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()

    if (value == null) throw Property.Exceptions.propertyValueCanNotBeNull()
    if (value.isInstanceOf[Iterable[_]]) throw new java.lang.IllegalArgumentException("not supported data type")
    if (value.isInstanceOf[Array[_]]) throw new java.lang.IllegalArgumentException("not supported data type")
    if (value.isInstanceOf[java.util.List[_]]) throw new java.lang.IllegalArgumentException("not supported data type")
    if (value.isInstanceOf[java.util.Map[_, _]]) throw new java.lang.IllegalArgumentException("not supported data type")

    if (key.toString.isEmpty) throw Property.Exceptions.propertyKeyCanNotBeEmpty()
    if (Graph.Hidden.isHidden(key.toString)) throw Property.Exceptions.propertyKeyCanNotBeAHiddenKey(Graph.Hidden.hide(key.toString))

  }
}

case class S2Property[V](element: S2Edge,
                         labelMeta: LabelMeta,
                         key: String,
                         v: V,
                         ts: Long) extends Property[V] {

  import CanInnerValLike._
  lazy val innerVal = anyToInnerValLike.toInnerVal(value)(element.innerLabel.schemaVersion)
  lazy val innerValWithTs = InnerValLikeWithTs(innerVal, ts)

  val value = castValue(v, labelMeta.dataType).asInstanceOf[V]

  def bytes: Array[Byte] = {
    innerVal.bytes
  }

  def bytesWithTs: Array[Byte] = {
    innerValWithTs.bytes
  }

  @volatile var isRemoved = false

  override def isPresent: Boolean = !isRemoved

  override def remove(): Unit = isRemoved = true

  override def hashCode(): Int = {
    MurmurHash3.stringHash(labelMeta.labelId + "," + labelMeta.id.get + "," + key + "," + value + "," + ts)
  }

  override def equals(other: Any): Boolean = other match {
    case p: Property[_] =>
      key == p.key() && v == p.value()
    case _ => false
  }

  override def toString(): String = {
//    Map("labelMeta" -> labelMeta.toString, "key" -> key, "value" -> value, "ts" -> ts).toString
    // vp[name->marko]
    s"p[${key}->${value}]"
  }

}
