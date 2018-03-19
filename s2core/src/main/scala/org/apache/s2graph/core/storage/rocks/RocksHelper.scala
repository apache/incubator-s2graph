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

package org.apache.s2graph.core.storage.rocks

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.s2graph.core.QueryParam

object RocksHelper {

  def intToBytes(value: Int): Array[Byte] = {
    val intBuffer = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder())
    intBuffer.clear()
    intBuffer.putInt(value)
    intBuffer.array()
  }

  def longToBytes(value: Long): Array[Byte] = {
    val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
    longBuffer.clear()
    longBuffer.putLong(value)
    longBuffer.array()
  }

  def bytesToInt(data: Array[Byte], offset: Int): Int = {
    if (data != null) {
      val intBuffer = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder())
      intBuffer.put(data, offset, 4)
      intBuffer.flip()
      intBuffer.getInt()
    } else 0
  }

  def bytesToLong(data: Array[Byte], offset: Int): Long = {
    if (data != null) {
      val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
      longBuffer.put(data, offset, 8)
      longBuffer.flip()
      longBuffer.getLong()
    } else 0L
  }

  case class ScanWithRange(cf: Array[Byte], startKey: Array[Byte], stopKey: Array[Byte], offset: Int, limit: Int)
  case class GetRequest(cf: Array[Byte], key: Array[Byte])

  type RocksRPC = Either[GetRequest, ScanWithRange]
}
