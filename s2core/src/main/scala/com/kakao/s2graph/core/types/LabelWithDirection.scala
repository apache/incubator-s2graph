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

package com.kakao.s2graph.core.types

import com.kakao.s2graph.core.GraphUtil
import com.kakao.s2graph.core.utils.logger
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/6/15.
 */
object LabelWithDirection {

  import HBaseType._

  def apply(compositeInt: Int): LabelWithDirection = {
    //      logger.debug(s"CompositeInt: $compositeInt")

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

case class LabelWithDirection(labelId: Int, dir: Int) extends HBaseSerializable {

  import HBaseType._

  assert(dir < (1 << bitsForDir))
  assert(labelId < (Int.MaxValue >> bitsForDir))

  lazy val labelBits = labelId << bitsForDir

  lazy val compositeInt = labelBits | dir

  def bytes = {
     Bytes.toBytes(compositeInt)
  }

  lazy val dirToggled = LabelWithDirection(labelId, GraphUtil.toggleDir(dir))

  def updateDir(newDir: Int) = LabelWithDirection(labelId, newDir)

  def isDirected = dir == 0 || dir == 1

  override def hashCode(): Int = compositeInt

  override def equals(other: Any): Boolean = {
    other match {
      case o: LabelWithDirection => hashCode == o.hashCode()
      case _ => false
    }
  }
}
