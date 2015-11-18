package s2.util

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
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

  // cache statistics
  def getStatsString: String = {
    s"$localHostname ${cache.stats().toString}"
  }

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
  }

  def withCacheAsync(key: String)(op: => Future[C])(implicit ec: ExecutionContext): Future[C] = {
    Option(cache.getIfPresent(key)) match {
      case Some(r) => Future.successful(r)
      case None =>
        op.map { r =>
          if (r.nonEmpty || config.negativeCache) {
            cache.put(key, r)
          }
          r
        }
    }
  }

  def purgeKey(key: String) = {
    cache.invalidate(key)
  }

  def contains(key: String): Boolean = {
    Option(cache.getIfPresent(key)).nonEmpty
  }
}
