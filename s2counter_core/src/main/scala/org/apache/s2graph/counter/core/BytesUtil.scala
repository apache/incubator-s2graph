package org.apache.s2graph.counter.core


trait BytesUtil {
  def getRowKeyPrefix(id: Int): Array[Byte]

  def toBytes(key: ExactKeyTrait): Array[Byte]
  def toBytes(eq: ExactQualifier): Array[Byte]
  def toBytes(tq: TimedQualifier): Array[Byte]

  def toExactQualifier(bytes: Array[Byte]): ExactQualifier
  def toTimedQualifier(bytes: Array[Byte]): TimedQualifier
}
