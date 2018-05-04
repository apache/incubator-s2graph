package org.apache.s2graph.core.model.fasttext


package fasttext

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import java.nio.{ByteBuffer, ByteOrder}

case class FastTextArgs(
                         magic: Int,
                         version: Int,
                         dim: Int,
                         ws: Int,
                         epoch: Int,
                         minCount: Int,
                         neg: Int,
                         wordNgrams: Int,
                         loss: Int,
                         model: Int,
                         bucket: Int,
                         minn: Int,
                         maxn: Int,
                         lrUpdateRate: Int,
                         t: Double,
                         size: Int,
                         nwords: Int,
                         nlabels: Int,
                         ntokens: Long,
                         pruneidxSize: Long) {

  def serialize: Array[Byte] = {
    val bb = ByteBuffer.allocate(92).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(magic)
    bb.putInt(version)
    bb.putInt(dim)
    bb.putInt(ws)
    bb.putInt(epoch)
    bb.putInt(minCount)
    bb.putInt(neg)
    bb.putInt(wordNgrams)
    bb.putInt(loss)
    bb.putInt(model)
    bb.putInt(bucket)
    bb.putInt(minn)
    bb.putInt(maxn)
    bb.putInt(lrUpdateRate)
    bb.putDouble(t)
    bb.putInt(size)
    bb.putInt(nwords)
    bb.putInt(nlabels)
    bb.putLong(ntokens)
    bb.putLong(pruneidxSize)
    bb.array()
  }

  override def toString: String = {
    s"""magic:        $magic
       |version:      $version
       |dim:          $dim
       |ws :          $ws
       |epoch:        $epoch
       |minCount:     $minCount
       |neg:          $neg
       |wordNgrams:   $wordNgrams
       |loss:         $loss
       |model:        $model
       |bucket:       $bucket
       |minn:         $minn
       |maxn:         $maxn
       |lrUpdateRate: $lrUpdateRate
       |t:            $t
       |size:         $size
       |nwords:       $nwords
       |nlabels:      $nlabels
       |ntokens:      $ntokens
       |pruneIdxSize: $pruneidxSize
       |""".stripMargin
  }

}

object FastTextArgs {

  private def getInt(implicit inputStream: InputStream, buffer: Array[Byte]): Int = {
    inputStream.read(buffer, 0, 4)
    ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  private def getLong(implicit inputStream: InputStream, buffer: Array[Byte]): Long = {
    inputStream.read(buffer, 0, 8)
    ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

  private def getDouble(implicit inputStream: InputStream, buffer: Array[Byte]): Double = {
    inputStream.read(buffer, 0, 8)
    ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).getDouble
  }

  def fromByteArray(ar: Array[Byte]): FastTextArgs =
    fromInputStream(new ByteArrayInputStream(ar))

  def fromInputStream(inputStream: InputStream): FastTextArgs = {
    implicit val is: InputStream = inputStream
    implicit val bytes: Array[Byte] = new Array[Byte](8)
    FastTextArgs(
      getInt, getInt, getInt, getInt, getInt, getInt, getInt, getInt, getInt, getInt,
      getInt, getInt, getInt, getInt, getDouble, getInt, getInt, getInt, getLong, getLong)
  }

  def main(args: Array[String]): Unit = {
    val args0 = FastTextArgs.fromInputStream(new FileInputStream("/Users/emeth.kim/d/g/fastText/dataset/sample.model.bin"))
    val serialized = args0.serialize
    val args1 = FastTextArgs.fromByteArray(serialized)

    println(args0)
    println(args1)
  }

}