package s2.models

import s2.util.{CollectionCache, CollectionCacheConfig}
import scalikejdbc.AutoSession

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 27..
 */
trait CachedDBModel[T] {
  implicit val s = AutoSession

  val cacheConfig: CollectionCacheConfig
  lazy val cache = new CollectionCache[Option[T]](cacheConfig)
}
