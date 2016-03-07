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

package s2.helper

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import s2.config.S2CounterConfig
import s2.util.Hashes

import scala.util.Try

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 19..
 */
class WithRedis(config: Config) {
  lazy val s2config = new S2CounterConfig(config)

  private val log = LoggerFactory.getLogger(getClass)

  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(150)
  poolConfig.setMaxIdle(50)
  poolConfig.setMaxWaitMillis(200)

  val jedisPools = s2config.REDIS_INSTANCES.map { case (host, port) =>
    new JedisPool(poolConfig, host, port)
  }

  def getBucketIdx(key: String): Int = {
    Hashes.murmur3(key) % jedisPools.size
  }

  def doBlockWithIndex[T](idx: Int)(f: Jedis => T): Try[T] = {
    Try {
      val pool = jedisPools(idx)

      var jedis: Jedis = null

      try {
        jedis = pool.getResource

        f(jedis)
      }
      catch {
        case e: JedisException =>
          pool.returnBrokenResource(jedis)

          jedis = null
          throw e
      }
      finally {
        if (jedis != null) {
          pool.returnResource(jedis)
        }
      }
    }
  }

  def doBlockWithKey[T](key: String)(f: Jedis => T): Try[T] = {
    doBlockWithIndex(getBucketIdx(key))(f)
  }
}
