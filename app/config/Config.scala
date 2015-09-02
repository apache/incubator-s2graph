package config

import play.api.Play

object Config {
  // HBASE
  lazy val HBASE_ZOOKEEPER_QUORUM = conf.getString("hbase.zookeeper.quorum").getOrElse("localhost")


  // HBASE CLIENT
  lazy val ASYNC_HBASE_CLIENT_FLUSH_INTERVAL = conf.getInt("async.hbase.client.flush.interval").getOrElse(1000).toShort
  lazy val RPC_TIMEOUT = conf.getInt("hbase.client.operation.timeout").getOrElse(1000)
  lazy val MAX_ATTEMPT = conf.getInt("hbase.client.operation.maxAttempt").getOrElse(3)

  // PHASE
  lazy val PHASE = conf.getString("phase").getOrElse("dev")
  lazy val conf = Play.current.configuration

  // CACHE
  lazy val CACHE_TTL_SECONDS = conf.getInt("cache.ttl.seconds").getOrElse(600)
  lazy val CACHE_MAX_SIZE = conf.getInt("cache.max.size").getOrElse(10000)

  //KAFKA
  lazy val KAFKA_METADATA_BROKER_LIST = conf.getString("kafka.metadata.broker.list").getOrElse("localhost")
  lazy val KAFKA_PRODUCER_POOL_SIZE = conf.getInt("kafka.producer.pool.size").getOrElse(0)
  lazy val KAFKA_LOG_TOPIC = s"s2graphIn${PHASE}"
  lazy val KAFKA_LOG_TOPIC_ASYNC = s"s2graphIn${PHASE}Async"
  lazy val KAFKA_FAIL_TOPIC = s"s2graphIn${PHASE}Failed"

  // use Keep-Alive
  lazy val USE_KEEP_ALIVE = conf.getBoolean("use.keep.alive").getOrElse(false)

  // is query or write
  lazy val IS_QUERY_SERVER = conf.getBoolean("is.query.server").getOrElse(true)
  lazy val IS_WRITE_SERVER = conf.getBoolean("is.write.server").getOrElse(true)


  // query limit per step
  lazy val QUERY_HARD_LIMIT = conf.getInt("query.hard.limit").getOrElse(300)

  // local queue actor
  lazy val LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE = conf.getInt("local.queue.actor.max.queue.size").getOrElse(10000)
  lazy val LOCAL_QUEUE_ACTOR_RATE_LIMIT = conf.getInt("local.queue.actor.rate.limit").getOrElse(1000)
}
