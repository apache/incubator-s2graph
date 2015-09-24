package s2.counter.core.v1

import java.util

import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{ColumnRangeFilter, FilterList, GetRequest, KeyValue}
import org.slf4j.LoggerFactory
import s2.config.S2CounterConfig
import s2.counter.core.ExactCounter.ExactValueMap
import s2.counter.core._
import s2.helper.{Management, WithAsyncHBaseNew, WithHBaseNew}
import s2.models.Counter
import s2.models.Counter.ItemType

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 8. 19..
 */
class ExactStorageHBaseV1(config: Config) extends ExactStorage {
  import ExactStorageHBaseV1._
  import TimedQualifier.IntervalUnit._

  private val log = LoggerFactory.getLogger(getClass)

  val intervalsMap = Map(MINUTELY -> ColumnFamily.SHORT, HOURLY -> ColumnFamily.SHORT,
    DAILY -> ColumnFamily.LONG, MONTHLY -> ColumnFamily.LONG, TOTAL -> ColumnFamily.LONG)

  lazy val s2config = new S2CounterConfig(config)

  private[counter] val withHBase = new WithHBaseNew(config)
  private[counter] val withAsyncHBase = new WithAsyncHBaseNew(config)
  private[counter] val hbaseManagement = new Management(config)

  private def getTableName(policy: Counter): String = {
    policy.hbaseTable.getOrElse(s2config.HBASE_TABLE_NAME)
  }
  
  override def get(policy: Counter,
                   items: Seq[String],
                   timeRange: Seq[(TimedQualifier, TimedQualifier)],
                   dimQuery: Map[String, Set[String]])
                  (implicit ex: ExecutionContext): Future[Seq[FetchedCountsGrouped]] = {

    val tableName = getTableName(policy)

    lazy val messageForLog = s"${policy.service}.${policy.action} $items $timeRange $dimQuery"

    val keys = {
      for {
        item <- items
      } yield {
        ExactKey(policy, item, checkItemType = true)
      }
    }

    val gets = {
      for {
        cf <- timeRange.map(t => intervalsMap(t._1.q)).distinct
        key <- keys
      } yield {
        val get = new GetRequest(tableName, BytesUtilV1.toBytes(key))
        get.family(cf.toString)
        get.filter(new FilterList({
          for {
            (from, to) <- timeRange
          } yield {
            new ColumnRangeFilter(
              BytesUtilV1.toBytes(from), true,
              BytesUtilV1.toBytes(to.copy(ts = to.ts + 1)), false)
          }
        }, FilterList.Operator.MUST_PASS_ONE))
        (key, cf, get)
      }
    }

//    println(s"$messageForLog $gets")

    withAsyncHBase[Seq[FetchedCountsGrouped]] { client =>
      val deferreds: Seq[Deferred[FetchedCounts]] = {
        for {
          (key, cf, get) <- gets
        } yield {
          client.get(get).addCallback { new Callback[FetchedCounts, util.ArrayList[KeyValue]] {
            override def call(kvs: util.ArrayList[KeyValue]): FetchedCounts = {
              val qualifierWithCounts = {
                for {
                  kv <- kvs
                  eq = BytesUtilV1.toExactQualifier(kv.qualifier()) if eq.checkDimensionEquality(dimQuery)
                } yield {
                  eq -> Bytes.toLong(kv.value())
                }
              }.toMap
//              println(s"$key $qualifierWithCounts")
              FetchedCounts(key, qualifierWithCounts)
            }
          }}
        }
      }
      Deferred.group(deferreds).addCallback { new Callback[Seq[FetchedCountsGrouped], util.ArrayList[FetchedCounts]] {
        override def call(arg: util.ArrayList[FetchedCounts]): Seq[FetchedCountsGrouped] = {
          val counts = {
            for {
              (key, fetchedGroup) <- Seq(arg: _*).groupBy(_.exactKey)
            } yield {
              fetchedGroup.reduce[FetchedCounts] { case (f1, f2) =>
                FetchedCounts(key, f1.qualifierWithCountMap ++ f2.qualifierWithCountMap)
              }
            }
          }.toSeq

          for {
            FetchedCounts(k, qualifierWithCountMap) <- counts
          } yield {
            FetchedCountsGrouped(k, qualifierWithCountMap.groupBy { case (eq, v) => (eq.tq.q, eq.dimKeyValues) })
          }
        }
      }}
    }
  }

  override def update(policy: Counter, counts: Seq[(ExactKeyTrait, ExactValueMap)]): Map[ExactKeyTrait, ExactValueMap] = {
    // increment mutation to hbase
    val increments = {
      for {
        (exactKey, values) <- counts
        inc = new Increment(BytesUtilV1.toBytes(exactKey))
      } yield {
        for {
          (eq, value) <- values
        } {
          inc.addColumn(intervalsMap.apply(eq.tq.q).toString.getBytes, BytesUtilV1.toBytes(eq), value)
        }
        // add column by dimension
        inc
      }
    }

    val results: Array[Object] = Array.fill(increments.size)(null)

    withHBase(getTableName(policy)) { table =>
      table.batch(increments, results)
    } match {
      case Failure(ex) =>
        log.error(s"${ex.getMessage}")
      case _ =>
    }

    assert(counts.length == results.length)

    for {
      ((exactKey, eqWithValue), result) <- counts.zip(results)
    } yield {
      val eqWithResult = result match {
        case r: Result =>
          for {
            (eq, value) <- eqWithValue
          } yield {
            val interval = eq.tq.q
            val cf = intervalsMap(interval)
            val result = Option(r.getColumnLatestCell(cf.toString.getBytes, BytesUtilV1.toBytes(eq))).map { cell =>
              Bytes.toLong(CellUtil.cloneValue(cell))
            }.getOrElse(-1l)
            eq -> result
          }
        case ex: Throwable =>
          log.error(s"${ex.getMessage}: $exactKey")
          Nil
        case _ =>
          log.error(s"result is null: $exactKey")
          Nil
      }
      (exactKey, eqWithResult.toMap)
    }
  }.toMap

  override def delete(policy: Counter, keys: Seq[ExactKeyTrait]): Unit = {
    withHBase(getTableName(policy)) { table =>
      table.delete {
        for {
          key <- keys
        } yield {
          new Delete(BytesUtilV1.toBytes(key))
        }
      }
    } match {
      case Failure(ex) =>
        log.error(ex.getMessage)
      case _ =>
    }
  }

  override def get(policy: Counter,
                   queries: Seq[(ExactKeyTrait, Seq[ExactQualifier])])
                  (implicit ex: ExecutionContext): Future[Seq[FetchedCounts]] = {

    val tableName = getTableName(policy)

    val gets = {
      for {
        (key, eqs) <- queries
        (cf, eqsGrouped) <- eqs.groupBy(eq => intervalsMap(eq.tq.q))
      } yield {
//        println(s"$key $eqsGrouped")
        val get = new GetRequest(tableName, BytesUtilV1.toBytes(key))
        get.family(cf.toString)
        get.qualifiers(eqsGrouped.map(BytesUtilV1.toBytes).toArray)
        (key, cf, get)
      }
    }

    withAsyncHBase[Seq[FetchedCounts]] { client =>
      val deferreds: Seq[Deferred[FetchedCounts]] = {
        for {
          (key, cf, get) <- gets
        } yield {
          client.get(get).addCallback { new Callback[FetchedCounts, util.ArrayList[KeyValue]] {
            override def call(kvs: util.ArrayList[KeyValue]): FetchedCounts = {
              val qualifierWithCounts = {
                for {
                  kv <- kvs
                  eq = BytesUtilV1.toExactQualifier(kv.qualifier())
                } yield {
                  eq -> Bytes.toLong(kv.value())
                }
              }.toMap
              FetchedCounts(key, qualifierWithCounts)
            }
          }}
        }
      }
      Deferred.group(deferreds).addCallback { new Callback[Seq[FetchedCounts], util.ArrayList[FetchedCounts]] {
        override def call(arg: util.ArrayList[FetchedCounts]): Seq[FetchedCounts] = arg
      }}
    }
  }

  override def insertBlobValue(policy: Counter, keys: Seq[BlobExactKey]): Seq[Boolean] = {
    val results: Array[Object] = Array.fill(keys.size)(null)

    val puts = keys.map { key =>
      val put = new Put(BytesUtilV1.toBytes(key))
      put.addColumn(blobCF, blobColumn, key.itemId.getBytes)
    }

    withHBase(getTableName(policy)) { table =>
      table.batch(puts, results)
    } match {
      case Failure(ex) =>
        log.error(s"${ex.getMessage}")
      case _ =>
    }

    for {
      (result, key) <- results.zip(keys)
    } yield {
      Option(result).map(_ => true).getOrElse {
        log.error(s"fail to insert blob value: $key")
        false
      }
    }
  }

  override def getBlobValue(policy: Counter, blobId: String): Option[String] = {
    lazy val messageForLog = s"${policy.service}.${policy.action}.$blobId"

    policy.itemType match {
      case ItemType.BLOB =>
        withHBase(getTableName(policy)) { table =>
          val rowKey = BytesUtilV1.toBytes(ExactKey(policy.id, policy.version, policy.itemType, blobId))
          val get = new Get(rowKey)
          get.addColumn(blobCF, blobColumn)
          table.get(get)
        } match {
          case Success(result) =>
            Option(result).filter(!_.isEmpty).map { rst =>
              Bytes.toString(rst.getValue(blobCF, blobColumn))
            }
          case Failure(ex) =>
            throw ex
        }
      case _ =>
        log.warn(s"is not blob type counter. $messageForLog")
        throw new Exception(s"is not blob type counter. $messageForLog")
    }
  }

  override def prepare(policy: Counter): Unit = {
    // create hbase table
    policy.hbaseTable.foreach { table =>
      if (!hbaseManagement.tableExists(s2config.HBASE_ZOOKEEPER_QUORUM, table)) {
        hbaseManagement.createTable(s2config.HBASE_ZOOKEEPER_QUORUM, table,
          ColumnFamily.values.map(_.toString).toList, 1)
        hbaseManagement.setTTL(s2config.HBASE_ZOOKEEPER_QUORUM, table, ColumnFamily.SHORT.toString, policy.ttl)
        policy.dailyTtl.foreach { i =>
          hbaseManagement.setTTL(s2config.HBASE_ZOOKEEPER_QUORUM, table, ColumnFamily.LONG.toString, i * 24 * 60 * 60)
        }
      }
    }
  }

  override def destroy(policy: Counter): Unit = {

  }
}

object ExactStorageHBaseV1 {
  object ColumnFamily extends Enumeration {
    type ColumnFamily = Value

    val SHORT = Value("s")
    val LONG = Value("l")
  }

  val blobCF = ColumnFamily.LONG.toString.getBytes
  val blobColumn = "b".getBytes
}