package s2.util

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import org.slf4j.LoggerFactory

import scala.language.{postfixOps, reflectiveCalls}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 7. 1..
 */
case class CollectionCacheConfig(maxSize: Int, ttl: Int, negativeCache: Boolean = false, negativeTTL: Int = 600)

class CollectionCache[C <: { def nonEmpty: Boolean; def isEmpty: Boolean } ](config: CollectionCacheConfig) {
  private val cache: Cache[String, C] = CacheBuilder.newBuilder()
    .expireAfterWrite(config.ttl, TimeUnit.SECONDS)
    .maximumSize(config.maxSize)
    .build[String, C]()

//  private lazy val cache = new SynchronizedLruMap[String, (C, Int)](config.maxSize)
  private lazy val className = this.getClass.getSimpleName

  private lazy val log = LoggerFactory.getLogger(this.getClass)
  val localHostname = InetAddress.getLocalHost.getHostName

  def size = cache.size
  val maxSize = config.maxSize

//  def currentTime: Int = System.currentTimeMillis() / 1000 toInt

  // cache statistics
  def getStatsString: String = {
    s"$localHostname ${cache.stats().toString}"
  }

//  private def withTTL(key: String, withStats: Boolean = true)(op: => C): C = {
//    lazy val current: Int = currentTime
//
//    if (withStats) stats.synchronized { stats.request += 1}
//
//    val cached = cache.get(key).filter { case (existed, ts) =>
//      val elapsed = current - ts
//      if ((existed.nonEmpty && elapsed > config.ttl) || (existed.isEmpty && elapsed > config.negativeTTL)) {
//        // expired
//        cache.remove(key)
//        if (withStats) stats.synchronized { stats.expired += 1}
//        false
//      } else {
//        if (withStats) stats.synchronized { stats.hit += 1}
//        // cache hit
//        true
//      }
//    }.map(_._1)
//
//    cached.getOrElse {
//      val r = op
//      if (r.nonEmpty || config.negativeCache) {
//        cache.put(key, (r, current))
//      }
//      r
//    }
//  }

  def withCache(key: String)(op: => C): C = {
    Option(cache.getIfPresent(key)) match {
      case Some(r) => r
      case None =>
        val r = op
        if (r.nonEmpty || config.negativeCache) {
          cache.put(key, r)
        }
        r
    }
//    withTTL(s"$className:$key")(op)
  }

  def purgeKey(key: String) = {
    cache.invalidate(key)
//    if (cache.contains(key)) cache.remove(key)
  }

  def contains(key: String): Boolean = {
    Option(cache.getIfPresent(key)).nonEmpty
//    cache.contains(key)
  }
}
