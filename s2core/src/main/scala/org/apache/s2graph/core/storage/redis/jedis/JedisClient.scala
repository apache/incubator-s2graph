package org.apache.s2graph.core.storage.redis.jedis

import com.typesafe.config.Config
import org.apache.s2graph.core.GraphUtil
import org.slf4j.LoggerFactory
import redis.clients.jedis._
import redis.clients.jedis.exceptions.JedisException

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Jan/07.
 */
class JedisClient(config: Config) {
  // TODO: refactor me
  lazy val instances: List[(String, Int)] = if (config.hasPath("redis.storage")) {
    (for {
      s <- config.getStringList("redis.storage")
    } yield {
        val sp = s.split(':')
        (sp(0), if (sp.length > 1) sp(1).toInt else 6379)
      }).toList
  } else List("localhost" -> 6379)


  private val log = LoggerFactory.getLogger(getClass)

  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(150)
  poolConfig.setMaxIdle(100)
  poolConfig.setMaxWaitMillis(200)

  val jedisPools = instances.map { case (host, port) =>
//    log.info(s">> jedisPool initialized : $host, $port")
    new JedisPool(poolConfig, host, port)
  }

  def getBucketIdx(key: String): Int = {
    GraphUtil.murmur3(key) % jedisPools.size
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

class JedisTransaction(passClient: Client) extends Transaction(passClient) {

  def evalWithLong(script: Array[Byte], keys: List[Array[Byte]], args: List[Array[Byte]]): Response[java.lang.Long] = {
    val params: Array[Array[Byte]] = (keys ++ args).toArray

    getClient(script).eval(script, Protocol.toByteArray(keys.length), params)

    return getResponse(BuilderFactory.LONG)
  }

  def evalWithString(script: Array[Byte], keys: List[Array[Byte]], args: List[Array[Byte]]): Response[java.lang.String] = {
    val params: Array[Array[Byte]] = (keys ++ args).toArray

    getClient(script).eval(script, Protocol.toByteArray(keys.length), params)

    return getResponse(BuilderFactory.STRING)
  }

}
