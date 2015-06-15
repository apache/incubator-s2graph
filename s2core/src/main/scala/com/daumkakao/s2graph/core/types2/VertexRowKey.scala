//package com.daumkakao.s2graph.core.types2
//
///**
// * Created by shon on 6/6/15.
// */
//object VertexRowKey extends HBaseDeserializable {
//  def fromBytes(bytes: Array[Byte],
//                offset: Int,
//                len: Int,
//                version: String = DEFAULT_VERSION): VertexRowKey = {
//    val id = VertexId.fromBytes(bytes, offset, len, version)
//    VertexRowKey(id)
//  }
//}
//case class VertexRowKey(id: VertexId) extends HBaseSerializable {
//  val bytes: Array[Byte] = id.bytes
//}
