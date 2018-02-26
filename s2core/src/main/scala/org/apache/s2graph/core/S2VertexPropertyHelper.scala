package org.apache.s2graph.core

import org.apache.s2graph.core.mysqls.ColumnMeta
import org.apache.s2graph.core.types.InnerValLike

object S2VertexPropertyHelper {
  def propertyValue(v: S2VertexLike, key: String): Option[InnerValLike] = {
    key match {
      case "id" => Option(v.innerId)
      case _ =>
        v.serviceColumn.metasInvMap.get(key).map(x => propertyValueInner(v, x))
    }
  }


  def propertyValuesInner(vertex: S2VertexLike, columnMetas: Seq[ColumnMeta] = Nil): Map[ColumnMeta, InnerValLike] = {
    if (columnMetas.isEmpty) {
      vertex.serviceColumn.metaPropsDefaultMap.map { case (columnMeta, defaultVal) =>
        columnMeta -> propertyValueInner(vertex, columnMeta)
      }
    } else {
      (ColumnMeta.reservedMetas ++ columnMetas).map { columnMeta =>
        columnMeta -> propertyValueInner(vertex, columnMeta)
      }.toMap
    }
  }

  def propertyValueInner(vertex: S2VertexLike, columnMeta: ColumnMeta): InnerValLike = {
    if (vertex.props.containsKey(columnMeta.name)) {
      vertex.props.get(columnMeta.name).innerVal
    } else {
      vertex.serviceColumn.metaPropsDefaultMap(columnMeta)
    }
  }

  def toColumnMetas(vertex: S2VertexLike, keys: Seq[String]): Seq[ColumnMeta] = {
    for {
      key <- keys
      columnMeta <- vertex.serviceColumn.metasInvMap.get(key)
    } yield columnMeta
  }
}
