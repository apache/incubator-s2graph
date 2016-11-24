package org.apache.s2graph.core

import java.util

import org.apache.s2graph.core.mysqls.ColumnMeta
import org.apache.s2graph.core.types.CanInnerValLike
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty, Vertex => TpVertex}

case class S2VertexProperty[V](element: TpVertex,
                               columnMeta: ColumnMeta,
                               key: String,
                               value: V) extends VertexProperty[V] {
  implicit val encodingVer = columnMeta.serviceColumn.schemaVersion
  val innerVal = CanInnerValLike.anyToInnerValLike.toInnerVal(value)
  def toBytes: Array[Byte] = {
    innerVal.bytes
  }

  override def properties[U](strings: String*): util.Iterator[Property[U]] = ???

  override def property[V](s: String, v: V): Property[V] = ???

  override def remove(): Unit = ???

  override def id(): AnyRef = ???

  override def isPresent: Boolean = ???
}