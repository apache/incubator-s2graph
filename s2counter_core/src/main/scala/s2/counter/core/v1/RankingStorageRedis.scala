package s2.counter.core.v1

import java.lang

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import redis.clients.jedis.Pipeline
import s2.counter.core.RankingCounter.RankingValueMap
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core.{RankingKey, RankingResult, RankingStorage}
import s2.helper.WithRedis
import s2.models.{Counter, CounterModel}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 22..
 */
class RankingStorageRedis(config: Config) extends RankingStorage {
  private[counter] val log = LoggerFactory.getLogger(this.getClass)
  private[counter] val withRedis = new WithRedis(config)

  val counterModel = new CounterModel(config)

  val TOTAL = "_total_"

  /**
   * ex1)
   * dimension = "age.32"
   * ex2)
   * dimension = "age.gender.32.m"
   *
   */
  private def makeBucket(rankingKey: RankingKey): String = {
    val policyId = rankingKey.policyId
    val q = rankingKey.eq.tq.q
    val ts = rankingKey.eq.tq.ts
    val dimension = rankingKey.eq.dimension
    if (dimension.nonEmpty) {
      s"$policyId.$q.$ts.$dimension"
    }
    else {
      s"$policyId.$q.$ts"
    }
  }

  override def getTopK(rankingKey: RankingKey, k: Int): Option[RankingResult] = {
    val bucket = makeBucket(rankingKey)
    withRedis.doBlockWithKey(bucket) { jedis =>
      jedis.zrevrangeByScoreWithScores(bucket, "+inf", "-inf", 0, k + 1).toSeq.map(t => (t.getElement, t.getScore))
    } match {
      case Success(values) =>
        if (values.nonEmpty) {
//          println(values)
          Some(RankingResult(values.find(_._1 == TOTAL).map(_._2).getOrElse(-1d), values.filter(_._1 != TOTAL).take(k)))
        }
        else {
          None
        }
      case Failure(ex) =>
        log.error(s"fail to get top k($ex). $rankingKey")
        None
    }
  }

  private def getTTL(policyId: Int, intervalUnit: IntervalUnit.IntervalUnit): Option[Int] = {
    counterModel.findById(policyId).flatMap { policy =>
      intervalUnit match {
        case IntervalUnit.MINUTELY => Some(policy.ttl)
        case IntervalUnit.HOURLY => Some(policy.ttl)
        // default daily ttl 31 day
        case IntervalUnit.DAILY => Some(policy.dailyTtl.getOrElse(31) * 24 * 3600)
        case IntervalUnit.MONTHLY => policy.dailyTtl
        case IntervalUnit.TOTAL => policy.dailyTtl
      }
    }
  }

  override def update(key: RankingKey, value: RankingValueMap, k: Int): Unit = {
    // update ranking by score
    val bucket = makeBucket(key)
    withRedis.doBlockWithKey(bucket) { jedis =>
      val pipeline = jedis.pipelined()
      updateItem(pipeline, bucket, key, value, k)
      pipeline.sync()
    } match {
      case Failure(ex) =>
        log.error(s"fail to update $key $value: $ex")
      case _ =>
    }
  }

  private def updateItem(pipeline: Pipeline, bucket: String, key: RankingKey, value: RankingValueMap, k: Int): Unit = {
    val topSeq = value.map { case (item, rv) =>
      // jedis client accept only java's double
      item -> rv.score.asInstanceOf[lang.Double]
    }.toSeq.sortBy(_._2).takeRight(k)
    pipeline.zadd(bucket, topSeq.toMap[String, lang.Double])
    pipeline.zincrby(bucket, value.mapValues(_.increment).values.sum, TOTAL)
    pipeline.zremrangeByRank(bucket, 0, -(k + 1))
    // if ttl defined, set expire time to bucket
    getTTL(key.policyId, key.eq.tq.q).foreach { ttl =>
      pipeline.expire(bucket, ttl)
    }
  }

  override def update(values: Seq[(RankingKey, RankingValueMap)], k: Int): Unit = {
    values.map { case (key, value) =>
      (makeBucket(key), key, value)
    }.groupBy { case (bucket, key, value) =>
      withRedis.getBucketIdx(bucket)
    }.foreach { case (idx, seq) =>
      withRedis.doBlockWithIndex(idx) { jedis =>
        val pipeline = jedis.pipelined()
        for {
          (bucket, key, value) <- seq
        } {
          updateItem(pipeline, bucket, key, value, k)
        }
        pipeline.sync()
      } match {
        case Failure(ex) =>
          log.error(s"fail to update multi $idx: $ex")
        case _ =>
      }
    }
  }

  override def delete(key: RankingKey): Unit = {
    val bucket = makeBucket(key)
    withRedis.doBlockWithKey(bucket) { jedis =>
      jedis.del(bucket)
    } match {
      case Success(deleted) =>
        log.info(s"success to delete $key")
      case Failure(ex) =>
        log.error(s"fail to delete $key")
    }
  }

  override def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)] = {
    keys.map { key =>
      (makeBucket(key), key)
    }.groupBy { case (bucket, key) =>
      withRedis.getBucketIdx(bucket)
    }.toSeq.par.flatMap { case (idx, seq) =>
      withRedis.doBlockWithIndex(idx) { jedis =>
        val pipeline = jedis.pipelined()
        val keyWithRespLs = {
          for {
            (bucket, rankingKey) <- seq
          } yield {
            (rankingKey, pipeline.zrevrangeByScoreWithScores(bucket, "+inf", "-inf", 0, k + 1))
          }
        }
        pipeline.sync()
        for {
          (rankingKey, resp) <- keyWithRespLs
        } yield {
          (rankingKey, resp.get().toSeq.map { t => (t.getElement, t.getScore)})
        }
      } match {
        case Success(keyWithValues) =>
          for {
            (rankingKey, values) <- keyWithValues
          } yield {
            val result = RankingResult(values.find(_._1 == TOTAL).map(_._2).getOrElse(-1d), values.filter(_._1 != TOTAL).take(k))
            (rankingKey, result)
          }
        case Failure(ex) =>
          Nil
      }
    }
  }.seq

  override def prepare(policy: Counter): Unit = {
    // do nothing
  }

  override def destroy(policy: Counter): Unit = {

  }

  override def ready(policy: Counter): Boolean = {
    // always return true
    true
  }
}
