package s2.helper

import com.stumbleupon.async.{Callback, Deferred}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.hbase.async.HBaseClient
import org.slf4j.LoggerFactory
import s2.config.S2CounterConfig

import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 19..
 */
class WithHBase(config: Config) {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  lazy val s2config = new S2CounterConfig(config)

  lazy val zkQuorum = s2config.HBASE_ZOOKEEPER_QUORUM
  lazy val defaultTableName = s2config.HBASE_TABLE_NAME

  logger.info(s"$zkQuorum, $defaultTableName")

  val hbaseConfig = HBaseConfiguration.create()
  s2config.getConfigMap("hbase").foreach { case (k, v) =>
    hbaseConfig.set(k, v)
  }

//  lazy val conn: HConnection = HConnectionManager.createConnection(hbaseConfig)
  lazy val conn: Connection = ConnectionFactory.createConnection(hbaseConfig)

  val writeBufferSize = 1024 * 1024 * 2   // 2MB

//  def apply[T](op: Table => T): Try[T] = {
//    Try {
//      val table = conn.getTable(TableName.valueOf(defaultTableName))
//      // do not keep failed operation in writer buffer
//      table.setWriteBufferSize(writeBufferSize)
//      try {
//        op(table)
//      } catch {
//        case e: Throwable =>
//          logger.error(s"Operation to table($defaultTableName) is failed: ${e.getMessage}")
//          throw e
//      } finally {
//        table.close()
//      }
//    }
//  }
  
  def apply[T](tableName: String)(op: Table => T): Try[T] = {
    Try {
      val table = conn.getTable(TableName.valueOf(tableName))
      // do not keep failed operation in writer buffer
      table.setWriteBufferSize(writeBufferSize)
      try {
        op(table)
      } catch {
        case ex: Exception =>
          logger.error(s"$ex: Operation to table($tableName) is failed")
          throw ex
      } finally {
        table.close()
      }
    }
  }
}

case class WithAsyncHBase(config: Config) {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  lazy val s2config = new S2CounterConfig(config)

  lazy val zkQuorum = s2config.HBASE_ZOOKEEPER_QUORUM

  val hbaseConfig = HBaseConfiguration.create()
  s2config.getConfigMap("hbase").foreach { case (k, v) =>
    hbaseConfig.set(k, v)
  }

//  lazy val conn: HConnection = HConnectionManager.createConnection(hbaseConfig)
  lazy val client: HBaseClient = new HBaseClient(zkQuorum)

  val writeBufferSize = 1024 * 1024 * 2   // 2MB

  def apply[T](op: HBaseClient => Deferred[T]): Future[T] = {
    val promise = Promise[T]()

    op(client).addCallback(new Callback[Unit, T] {
      def call(arg: T): Unit = {
        promise.success(arg)
      }
    }).addErrback(new Callback[Unit, Exception] {
      def call(ex: Exception): Unit = {
        promise.failure(ex)
      }
    })
    promise.future
  }
}
