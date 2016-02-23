package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.types.{InnerVal, InnerValLike, VertexId}
import com.kakao.s2graph.core.{QueryParam, Vertex}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

class VertexDeserializable extends Deserializable[Vertex] {
  def fromKeyValuesInner[T: CanSKeyValue](queryParam: QueryParam,
                                     _kvs: Seq[T],
                                     version: String,
                                     cacheElementOpt: Option[Vertex]): Vertex = {

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

    val kv = kvs.head
    val (vertexId, _) = VertexId.fromBytes(kv.row, 0, kv.row.length, version)

    var maxTs = Long.MinValue
    val propsMap = new collection.mutable.HashMap[Int, InnerValLike]
    val belongLabelIds = new ListBuffer[Int]

    for {
      kv <- kvs
    } {
      val propKey =
        if (kv.qualifier.length == 1) kv.qualifier.head.toInt
        else Bytes.toInt(kv.qualifier)

      val ts = kv.timestamp
      if (ts > maxTs) maxTs = ts

      if (Vertex.isLabelId(propKey)) {
        belongLabelIds += Vertex.toLabelId(propKey)
      } else {
        val v = kv.value
        val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
        propsMap += (propKey -> value)
      }
    }
    assert(maxTs != Long.MinValue)
    Vertex(vertexId, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds)
  }
}

