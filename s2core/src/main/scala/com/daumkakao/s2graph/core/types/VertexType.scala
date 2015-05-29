package com.daumkakao.s2graph.core.types


/**
 * Created by shon on 5/29/15.
 */
object VertexType {
  val ttsForActivity = 60 * 60 * 24 * 30
  val delimiter = "|"
  val seperator = ":"
  val bytesForMurMur = 2
  val bitsForDir = 2
  val bytesForOp = 3
  val bitsForLenWithDir = 5
  val bitsForDirWithLen = 2
  val bitsForOp = 3

  val bitForPropMode = 1
  val bitForByte = 7

  object VertexRowKey {
    def apply(bytes: Array[Byte], offset: Int): VertexRowKey = {
      VertexRowKey(CompositeId(bytes, offset, isEdge = false, useHash = true))
    }
  }

  case class VertexRowKey(id: CompositeId) {
    lazy val bytes = id.bytes
  }

  object VertexQualifier {
    def apply(bytes: Array[Byte], offset: Int, len: Int): VertexQualifier = {
      VertexQualifier(bytes(offset))
    }
  }
  case class VertexQualifier(propKey: Byte) {
    /** assumes that propeKey is only positive byte */
    lazy val bytes = Array.fill(1)(propKey)
  }

}
