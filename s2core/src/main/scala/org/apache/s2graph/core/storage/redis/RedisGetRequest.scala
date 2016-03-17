package org.apache.s2graph.core.storage.redis

import org.apache.s2graph.core.GraphUtil

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */

trait RedisRPC {
  val key: Array[Byte]
}

case class RedisGetRequest(k: Array[Byte]) extends RedisRPC {
  /**
   * For degree edge key(not sorted set key/value case)
   */
  override val key = k

  assert(key.length > 0)

  var isIncludeDegree: Boolean = true

  var timeout: Long = _
  var count: Int = _
  var offset: Int = _

  var min: Array[Byte] = _
  var minInclusive: Boolean = _
  var max: Array[Byte] = _
  var maxInclusive: Boolean = _

  var minTime: Long = _
  var maxTime: Long = _

  def setTimeout(time: Long): RedisGetRequest = {
    this.timeout = time
    this
  }
  def setCount(count: Int): RedisGetRequest = {
    this.count = count
    this
  }
  def setOffset(offset: Int): RedisGetRequest = {
    this.offset = offset
    this
  }

  // for `interval`
  def setFilter(min: Array[Byte],
                minInclusive: Boolean,
                max: Array[Byte],
                maxInclusive: Boolean,
                minTime: Long = -1,
                maxTime: Long = -1): RedisGetRequest = {

    this.min = min; this.minInclusive = minInclusive; this.max = max; this.maxInclusive = maxInclusive; this.minTime = minTime; this.maxTime = maxTime;
    this
  }

  override def toString() = {
    s"[RedisGetRequest] Key : ${GraphUtil.bytesToHexString(key)}"
  }

}


case class RedisSnapshotGetRequest(k: Array[Byte]) extends RedisRPC {
  override val key = k
  assert(key.length > 0)
}

