package org.apache.s2graph.core

import java.util
import java.util.function.BiConsumer

import org.apache.s2graph.core.S2Vertex.Props
import org.apache.s2graph.core.mysqls.ColumnMeta
import org.apache.s2graph.core.types.VertexId

class S2VertexBuilder(vertex: S2VertexLike) {
  def defaultProps: util.HashMap[String, S2VertexProperty[_]] = {
    val default = S2Vertex.EmptyProps
    val newProps = new S2VertexProperty(vertex, ColumnMeta.lastModifiedAtColumn, ColumnMeta.lastModifiedAtColumn.name, vertex.ts)
    default.put(ColumnMeta.lastModifiedAtColumn.name, newProps)
    default
  }

  def copyVertex(graph: S2GraphLike = vertex.graph,
                 id: VertexId = vertex.id,
                 ts: Long = vertex.ts,
                 props: Props = vertex.props,
                 op: Byte = vertex.op,
                 belongLabelIds: Seq[Int] = vertex.belongLabelIds): S2VertexLike = {
    val newProps = S2Vertex.EmptyProps
    val v = new S2Vertex(graph, id, ts, newProps, op, belongLabelIds)

    props.forEach(new BiConsumer[String, S2VertexProperty[_]] {
      override def accept(t: String, u: S2VertexProperty[_]) = {
        v.property(t, u.value)
      }
    })

    v
  }
}
