package org.apache.s2graph.core

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.CanInnerValLike
import org.apache.tinkerpop.gremlin.structure.{Element, Property}


case class S2Property[V](element: Element,
                         labelMeta: LabelMeta,
                         key: String,
                         value: V,
                         ts: Long = System.currentTimeMillis()) extends Property[V] {

  import CanInnerValLike._
  lazy val innerVal = anyToInnerValLike.toInnerVal(value, labelMeta.label.schemaVersion)

  def bytes: Array[Byte] = {
    innerVal.bytes
  }

  def bytesWithTs: Array[Byte] = {
    Bytes.add(innerVal.bytes, Bytes.toBytes(ts))
  }

  override def isPresent: Boolean = ???

  override def remove(): Unit = ???
}