package s2.counter.core.v1

import com.typesafe.config.Config
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{ColumnRangeFilter, FilterList}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import s2.config.S2CounterConfig
import s2.counter.core.ExactCounter.ExactValueMap
import s2.counter.core._
import s2.helper.{Management, WithHBase}
import s2.models.Counter
import s2.models.Counter.ItemType

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 10. 1..
 */
class ExactStorageHBase(config: Config) extends ExactStorage {
  import ExactStorageHBase._

  private val log = LoggerFactory.getLogger(getClass)

  lazy val s2config = new S2CounterConfig(config)

  private[counter] val withHBase = new WithHBase(config)
  private[counter] val hbaseManagement = new Management(config)

  private def getTableName(policy: Counter): String = {
    policy.hbaseTable.getOrElse(s2config.HBASE_TABLE_NAME)
  }

  override def get(policy: Counter,
                   items: Seq[String],
                   timeRange: Seq[(TimedQualifier, TimedQualifier)])
                  (implicit ec: ExecutionContext): Future[Seq[FetchedCounts]] = {
    lazy val messageForLog = s"${policy.service}.${policy.action} $items $timeRange"

    val keys = {
      for {
        item <- items
      } yield {
        ExactKey(policy, item, checkItemType = true)
      }
    }

    val gets = {
      for {
        key <- keys
      } yield {
        val get = new Get(BytesUtilV1.toBytes(key))
        timeRange.map(t => intervalsMap(t._1.q)).distinct.foreach { cf =>
          get.addFamily(cf.toString.getBytes)
        }
        get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, {
          for {
            (from, to) <- timeRange
          } yield {
            new ColumnRangeFilter(
              BytesUtilV1.toBytes(from), true,
              BytesUtilV1.toBytes(to.copy(ts = to.ts + 1)), false)
          }
        }))
      }
    }

    //    println(s"$messageForLog $gets")
    Future {
      withHBase(getTableName(policy)) { table =>
        for {
          (rst, key) <- table.get(gets).zip(keys) if !rst.isEmpty
        } yield {
          val qualifierWithCounts = {
            for {
              cell <- rst.listCells()
              eq = BytesUtilV1.toExactQualifier(CellUtil.cloneQualifier(cell))
            } yield {
              eq -> Bytes.toLong(CellUtil.cloneValue(cell))
            }
          }.toMap
          FetchedCounts(key, qualifierWithCounts)
        }
      } match {
        case Success(rst) => rst
        case Failure(ex) =>
          log.error(s"$ex: $messageForLog")
          Nil
      }
    }
  }

  override def get(policy: Counter,
                   items: Seq[String],
                   timeRange: Seq[(TimedQualifier, TimedQualifier)],
                   dimQuery: Map[String, Set[String]])
                  (implicit ec: ExecutionContext): Future[Seq[FetchedCountsGrouped]] = {
    get(policy, items, timeRange).map { fetchedLs =>
      for {
        FetchedCounts(exactKey, qualifierWithCountMap) <- fetchedLs
      } yield {
        val intervalWithCountMap = qualifierWithCountMap
          .filter { case (eq, v) => eq.checkDimensionEquality(dimQuery) }
          .groupBy { case (eq, v) => (eq.tq.q, eq.dimKeyValues) }
        FetchedCountsGrouped(exactKey, intervalWithCountMap)
      }
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
                  (implicit ec: ExecutionContext): Future[Seq[FetchedCounts]] = {
    lazy val messageForLog = s"${policy.service}.${policy.action} $queries"

    val gets = {
      for {
        (key, eqs) <- queries
      } yield {
        //        println(s"$key $eqsGrouped")
        val get = new Get(BytesUtilV1.toBytes(key))

        for {
          eq <- eqs
        } {
          val cf = intervalsMap(eq.tq.q)
          get.addColumn(cf.toString.getBytes, BytesUtilV1.toBytes(eq))
        }
        get
      }
    }

    Future {
      withHBase(getTableName(policy)) { table =>
        for {
          (rst, key) <- table.get(gets).zip(queries.map(_._1)) if !rst.isEmpty
        } yield {
          val qualifierWithCounts = {
            for {
              cell <- rst.listCells()
              eq = BytesUtilV1.toExactQualifier(CellUtil.cloneQualifier(cell))
            } yield {
              eq -> Bytes.toLong(CellUtil.cloneValue(cell))
            }
          }.toMap
          FetchedCounts(key, qualifierWithCounts)
        }
      } match {
        case Success(rst) => rst.toSeq
        case Failure(ex) =>
          log.error(s"$ex: $messageForLog")
          Nil
      }
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

  override def ready(policy: Counter): Boolean = {
    policy.hbaseTable.map { table =>
      hbaseManagement.tableExists(s2config.HBASE_ZOOKEEPER_QUORUM, table)
    }.getOrElse(true)
  }
}

object ExactStorageHBase {
  import TimedQualifier.IntervalUnit._

  object ColumnFamily extends Enumeration {
    type ColumnFamily = Value

    val SHORT = Value("s")
    val LONG = Value("l")
  }

  val blobCF = ColumnFamily.LONG.toString.getBytes
  val blobColumn = "b".getBytes

  val intervalsMap = Map(
    MINUTELY -> ColumnFamily.SHORT,
    HOURLY -> ColumnFamily.SHORT,
    DAILY -> ColumnFamily.LONG,
    MONTHLY -> ColumnFamily.LONG,
    TOTAL -> ColumnFamily.LONG
  )
}
