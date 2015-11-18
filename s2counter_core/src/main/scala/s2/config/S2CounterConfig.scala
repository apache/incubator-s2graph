package s2.config

import com.typesafe.config.Config

import scala.collection.JavaConversions._

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 3. 2..
 */
class S2CounterConfig(config: Config) extends ConfigFunctions(config) {
  // HBase
  lazy val HBASE_ZOOKEEPER_QUORUM = getOrElse("hbase.zookeeper.quorum", "")
  lazy val HBASE_TABLE_NAME = getOrElse("hbase.table.name", "s2counter")
  lazy val HBASE_TABLE_POOL_SIZE = getOrElse("hbase.table.pool.size", 100)
  lazy val HBASE_CONNECTION_POOL_SIZE = getOrElse("hbase.connection.pool.size", 10)

  lazy val HBASE_CLIENT_IPC_POOL_SIZE = getOrElse("hbase.client.ipc.pool.size", 5)
  lazy val HBASE_CLIENT_MAX_TOTAL_TASKS = getOrElse("hbase.client.max.total.tasks", 100)
  lazy val HBASE_CLIENT_MAX_PERSERVER_TASKS = getOrElse("hbase.client.max.perserver.tasks", 5)
  lazy val HBASE_CLIENT_MAX_PERREGION_TASKS = getOrElse("hbase.client.max.perregion.tasks", 1)
  lazy val HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = getOrElse("hbase.client.scanner.timeout.period", 300)
  lazy val HBASE_CLIENT_OPERATION_TIMEOUT = getOrElse("hbase.client.operation.timeout", 100)
  lazy val HBASE_CLIENT_RETRIES_NUMBER = getOrElse("hbase.client.retries.number", 1)

  // MySQL
  lazy val DB_DEFAULT_DRIVER = getOrElse("db.default.driver", "com.mysql.jdbc.Driver")
  lazy val DB_DEFAULT_URL = getOrElse("db.default.url", "")
  lazy val DB_DEFAULT_USER = getOrElse("db.default.user", "graph")
  lazy val DB_DEFAULT_PASSWORD = getOrElse("db.default.password", "graph")

  // REDIS
  lazy val REDIS_INSTANCES = (for {
    s <- config.getStringList("redis.instances")
  } yield {
    val sp = s.split(':')
    (sp(0), if (sp.length > 1) sp(1).toInt else 6379)
  }).toList

  // graph
  lazy val GRAPH_URL = getOrElse("s2graph.url", "http://localhost:9000")

  // Cache
  lazy val CACHE_TTL_SECONDS = getOrElse("cache.ttl.seconds", 600)
  lazy val CACHE_MAX_SIZE = getOrElse("cache.max.size", 10000)
  lazy val CACHE_NEGATIVE_TTL_SECONDS = getOrElse("cache.negative.ttl.seconds", CACHE_TTL_SECONDS)
}
