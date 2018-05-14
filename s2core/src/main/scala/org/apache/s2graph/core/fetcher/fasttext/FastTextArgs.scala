/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.fetcher.fasttext

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
