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

  case class ScanWithRange(queryParam: QueryParam, startKey: Array[Byte], stopKey: Array[Byte], offset: Int, limit: Int)
  case class GetRequest(cf: Array[Byte], key: Array[Byte])

  type RocksRPC = Either[GetRequest, ScanWithRange]
}
