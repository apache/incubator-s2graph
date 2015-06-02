package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 5/29/15.
 */
object LabelWithDirection {
  import VertexType._
  val maxBytes = Bytes.toBytes(Int.MaxValue)
  def apply(compositeInt: Int): LabelWithDirection = {
    //      play.api.Logger.debug(s"CompositeInt: $compositeInt")

    val dir = compositeInt & ((1 << bitsForDir) - 1)
    val labelId = compositeInt >> bitsForDir
    LabelWithDirection(labelId, dir)
  }
  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }
  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }
}
case class LabelWithDirection(labelId: Int, dir: Int) {
  import VertexType._
  assert(dir < (1 << bitsForDir))
  assert(labelId < (Int.MaxValue >> bitsForDir))

  val labelBits = labelId << bitsForDir

  lazy val compositeInt = labelBits | dir
  lazy val bytes = Bytes.toBytes(compositeInt)
  lazy val dirToggled = LabelWithDirection(labelId, GraphUtil.toggleDir(dir))
  def updateDir(newDir: Int) = LabelWithDirection(labelId, newDir)

}
