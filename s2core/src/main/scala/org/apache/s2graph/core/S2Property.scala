package org.apache.s2graph.core


import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerValLikeWithTs, CanInnerValLike}
import org.apache.tinkerpop.gremlin.structure.{Property}

import scala.util.hashing.MurmurHash3


case class S2Property[V](element: Edge,
                         labelMeta: LabelMeta,
                         key: String,
                         value: V,
                         ts: Long) extends Property[V] {

  import CanInnerValLike._
  lazy val innerVal = anyToInnerValLike.toInnerVal(value)(element.innerLabel.schemaVersion)
  lazy val innerValWithTs = InnerValLikeWithTs(innerVal, ts)

  def bytes: Array[Byte] = {
    innerVal.bytes
  }

  def bytesWithTs: Array[Byte] = {
    innerValWithTs.bytes
  }

  override def isPresent: Boolean = ???

  override def remove(): Unit = ???

  override def hashCode(): Int = {
    MurmurHash3.stringHash(labelMeta.labelId + "," + labelMeta.id.get + "," + key + "," + value + "," + ts)
  }

  override def equals(other: Any): Boolean = other match {
    case p: S2Property[_] =>
      labelMeta.labelId == p.labelMeta.labelId &&
      labelMeta.seq == p.labelMeta.seq &&
      key == p.key && value == p.value && ts == p.ts
    case _ => false
  }

  override def toString(): String = {
    Map("labelMeta" -> labelMeta.toString, "key" -> key, "value" -> value, "ts" -> ts).toString
  }
}