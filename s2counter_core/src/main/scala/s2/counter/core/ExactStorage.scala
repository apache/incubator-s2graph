package s2.counter.core

import s2.counter.core.ExactCounter.ExactValueMap
import s2.models.Counter

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by shon on 8/12/15.
 */
trait ExactStorage {
  // for range query
  def get(policy: Counter,
          items: Seq[String],
          timeRange: Seq[(TimedQualifier, TimedQualifier)],
          dimQuery: Map[String, Set[String]])
         (implicit ex: ExecutionContext): Future[Seq[FetchedCountsGrouped]]
  // for query exact qualifier
  def get(policy: Counter,
          queries: Seq[(ExactKeyTrait, Seq[ExactQualifier])])
         (implicit ex: ExecutionContext): Future[Seq[FetchedCounts]]
  def update(policy: Counter, counts: Seq[(ExactKeyTrait, ExactValueMap)]): Map[ExactKeyTrait, ExactValueMap]
  def delete(policy: Counter, keys: Seq[ExactKeyTrait]): Unit
  def insertBlobValue(policy: Counter, keys: Seq[BlobExactKey]): Seq[Boolean]
  def getBlobValue(policy: Counter, blobId: String): Option[String]
  def prepare(policy: Counter): Unit
  def destroy(policy: Counter): Unit
}
