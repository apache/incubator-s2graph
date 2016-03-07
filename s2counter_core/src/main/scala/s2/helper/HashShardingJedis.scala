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

/**
 * Created by jay on 14. 10. 31..
 */
class HashShardingJedis(config: Config) {
  lazy val s2config = new S2CounterConfig(config)

  private val log = LoggerFactory.getLogger(getClass)

  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(150)
  poolConfig.setMaxIdle(50)
  poolConfig.setMaxWaitMillis(200)

  val jedisPools = s2config.REDIS_INSTANCES.map { case (host, port) =>
    new JedisPool(poolConfig, host, port)
  }
  val jedisPoolSize = jedisPools.size

  def getJedisPool(idx: Int): JedisPool = {
    if(idx >= jedisPoolSize)
      null
    else
      jedisPools(idx)
  }

  def getJedisPoolWithBucketname2(bucket: String): JedisPool = {
    val hashedValue = Hashes.murmur3(bucket)
    val idx = hashedValue % jedisPoolSize
    getJedisPool(idx)
  }

  def getJedisPoolWithBucketname(bucket: String): (JedisPool, JedisPool) = {
    val hashedValue = Hashes.murmur3(bucket)
    val idx = hashedValue % jedisPoolSize
    val secondaryIdx = if (jedisPoolSize <= 1) {
      throw new Exception("too small sharding pool <= 1")
    } else {
      val newIdx = (hashedValue / jedisPoolSize) % (jedisPoolSize -1)
      if(newIdx < idx) {
        newIdx
      } else {
        newIdx +1
      }
    }
    (getJedisPool(idx), getJedisPool(secondaryIdx))
  }

  def doBlockWithJedisInstace(f : Jedis => Any, fallBack : => Any, jedis : Jedis) = {
    try {
      f(jedis)
    }
    catch {
      case e:JedisException => {
        fallBack
      }
    }
  }

  def doBlockWithBucketName(f : Jedis => Any, fallBack : => Any, bucket : String) = {
//    Logger.debug(s"start jedis do block")
    //val (jedis_pool1, jedis_pool2) = getJedisPoolWithBucketname(bucket)
    val jedis_pool1= getJedisPoolWithBucketname2(bucket)
//    if(jedis_pool1 != null && jedis_pool2 != null) {
    if(jedis_pool1 != null) {
      var jedis1: Jedis = null
//      var jedis2: Jedis = null
      try {
        jedis1 = jedis_pool1.getResource()
//        jedis2 = jedis_pool2.getResource()
        log.info(s">> Jedis Pool Active Num : ${jedis_pool1.getNumActive}")

        /* val f1 = Future(f(jedis1))
        val f2 = Future(f(jedis2))

        val mixedFuture = Future.sequence(List(f1,f2))   */

        val r1 = f(jedis1)
        //val r2 = f(jedis2)

        r1
      }
      catch {
        case e:JedisException => {
//          Logger.debug(s"following exception catched")
//          Logger.debug(s"${e}")
          jedis_pool1.returnBrokenResource(jedis1)
//          jedis_pool2.returnBrokenResource(jedis2)

          jedis1 = null
//          jedis2 = null
          fallBack
        }
      }
      finally {
        if (jedis1 != null) jedis_pool1.returnResource(jedis1)
//        if (jedis2 != null) jedis_pool2.returnResource(jedis2)
      }
    }
    else{
//      Logger.debug(s"fallback executed")
      fallBack
    }
  }
  
  def doBlockWithKey[T](key: String)(f: Jedis => T)(fallBack: => T) = {
//    Logger.debug(s"start jedis do block")
    val (jedis_pool1, jedis_pool2) = getJedisPoolWithBucketname(key)
    if(jedis_pool1 != null && jedis_pool2 != null) {
      var jedis1: Jedis = null
      var jedis2: Jedis = null
      try {
        jedis1 = jedis_pool1.getResource()
        jedis2 = jedis_pool2.getResource()

        /* val f1 = Future(f(jedis1))
        val f2 = Future(f(jedis2))

        val mixedFuture = Future.sequence(List(f1,f2))   */

        val r1 = f(jedis1)
        //val r2 = f(jedis2)

        r1
      }
      catch {
        case e:JedisException => {
//          Logger.debug(s"following exception catched")
//          Logger.debug(s"${e}")
          jedis_pool1.returnBrokenResource(jedis1)
          jedis_pool2.returnBrokenResource(jedis2)

          jedis1 = null
          jedis2 = null
          fallBack
        }
      }
      finally {
        if (jedis1 != null) jedis_pool1.returnResource(jedis1)
        if (jedis2 != null) jedis_pool2.returnResource(jedis2)
      }
    }
    else{
//      Logger.debug(s"fallback executed")
      fallBack
    }
  }
}
