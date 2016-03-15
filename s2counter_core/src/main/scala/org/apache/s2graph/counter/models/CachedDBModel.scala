package org.apache.s2graph.counter.models

import org.apache.s2graph.counter.util.{CollectionCache, CollectionCacheConfig}
import scalikejdbc.AutoSession

trait CachedDBModel[T] {
  implicit val s = AutoSession

  val cacheConfig: CollectionCacheConfig
  lazy val cache = new CollectionCache[Option[T]](cacheConfig)
}
