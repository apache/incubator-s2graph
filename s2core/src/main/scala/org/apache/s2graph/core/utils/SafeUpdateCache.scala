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

package org.apache.s2graph.core.utils

import java.io._
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.CacheBuilder
import com.google.common.hash.Hashing
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SafeUpdateCache {

  case class CacheKey(key: String)

  def serialise(value: AnyRef): Try[Array[Byte]] = {
    import scala.pickling.Defaults._
    import scala.pickling.binary._

    val result = Try(value.pickle.value)
    result.failed.foreach { e =>
      logger.syncInfo(s"[Serialise failed]: ${value}, ${e}")
    }

    result
  }

  def deserialise(bytes: Array[Byte]): Try[AnyRef] = {
    import scala.pickling.Defaults._
    import scala.pickling.binary._

    Try(BinaryPickle(bytes).unpickle[AnyRef])
  }

  def fromBytes(cache: SafeUpdateCache, bytes: Array[Byte]): Unit = {
    import org.apache.hadoop.io.WritableUtils

    val bais = new ByteArrayInputStream(bytes)
    val input = new DataInputStream(bais)

    try {
      val size = WritableUtils.readVInt(input)
      (1 to size).foreach { ith =>
        val cacheKey = WritableUtils.readVLong(input)
        val value = deserialise(WritableUtils.readCompressedByteArray(input))
        value.foreach { dsv =>
          cache.putInner(cacheKey, dsv)
        }
      }
    } finally {
      bais.close()
      input.close()
    }
  }
}

class SafeUpdateCache(maxSize: Int,
                      ttl: Int)
                     (implicit executionContext: ExecutionContext) {

  import java.lang.{Long => JLong}
  import SafeUpdateCache._

  private val cache = CacheBuilder.newBuilder().maximumSize(maxSize)
    .build[JLong, (AnyRef, Int, AtomicBoolean)]()

  private def toCacheKey(key: String): Long = {
    Hashing.murmur3_128().hashBytes(key.getBytes("UTF-8")).asLong()
  }

  private def toTs() = (System.currentTimeMillis() / 1000).toInt

  def put(key: String, value: AnyRef, broadcast: Boolean = false): Unit = {
    val cacheKey = toCacheKey(key)
    cache.put(cacheKey, (value, toTs, new AtomicBoolean(false)))
  }

  def putInner(cacheKey: Long, value: AnyRef): Unit = {
    cache.put(cacheKey, (value, toTs, new AtomicBoolean(false)))
  }

  def invalidate(key: String, broadcast: Boolean = true) = {
    val cacheKey = toCacheKey(key)
    cache.invalidate(cacheKey)
  }

  def invalidateInner(cacheKey: Long): Unit = {
    cache.invalidate(cacheKey)
  }

  def withCache[T <: AnyRef](key: String, broadcast: Boolean)(op: => T): T = {
    val cacheKey = toCacheKey(key)
    val cachedValWithTs = cache.getIfPresent(cacheKey)

    if (cachedValWithTs == null) {
      val value = op
      // fetch and update cache.
      put(key, value, broadcast)
      value
    } else {
      val (_cachedVal, updatedAt, isUpdating) = cachedValWithTs
      val cachedVal = _cachedVal.asInstanceOf[T]

      if (toTs() < updatedAt + ttl) cachedVal // in cache TTL
      else {
        val running = isUpdating.getAndSet(true)

        if (running) cachedVal
        else {
          val value = op
          Future(value)(executionContext) onComplete {
            case Failure(ex) =>
              put(key, cachedVal, false)
              logger.error(s"withCache update failed: $cacheKey", ex)
            case Success(newValue) =>
              put(key, newValue, broadcast = (broadcast && newValue != cachedVal))
              logger.info(s"withCache update success: $cacheKey")
          }

          cachedVal
        }
      }
    }
  }

  def invalidateAll() = cache.invalidateAll()

  def asMap() = cache.asMap()

  def get(key: String) = cache.asMap().get(toCacheKey(key))

  def toBytes(): Array[Byte] = {
    import org.apache.hadoop.io.WritableUtils
    val baos = new ByteArrayOutputStream()
    val output = new DataOutputStream(baos)

    try {
      val m = cache.asMap()
      val size = m.size()

      WritableUtils.writeVInt(output, size)
      for (key <- m.keys) {
        val (value, _, _) = m.get(key)
        WritableUtils.writeVLong(output, key)
        serialise(value).foreach { sv =>
          WritableUtils.writeCompressedByteArray(output, sv)
        }
      }
      output.flush()
      baos.toByteArray
    } finally {
      baos.close()
      output.close()
    }
  }

  def shutdown() = {}
}

