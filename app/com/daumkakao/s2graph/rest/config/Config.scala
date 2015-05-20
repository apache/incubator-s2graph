package com.daumkakao.s2graph.rest.config

import java.util.concurrent.TimeUnit
import com.codahale.metrics.{Slf4jReporter, Metric, ConsoleReporter, MetricRegistry}
import org.slf4j.LoggerFactory
import play.api.{Play, Logger}

object Config {
  val conf = Play.current.configuration

  // PHASE
  lazy val PHASE = conf.getString("phase").getOrElse("dev")

  // CACHE
  lazy val CACHE_TTL_SECONDS = conf.getInt("cache.ttl.seconds").getOrElse(600)
  lazy val CACHE_MAX_SIZE = conf.getInt("cache.max.size").getOrElse(10000)

  //KAFKA
  lazy val KAFKA_METADATA_BROKER_LIST = conf.getString("kafka.metadata.broker.list").getOrElse("localhost")
  lazy val KAFKA_PRODUCER_POOL_SIZE = conf.getInt("kafka.producer.pool.size").getOrElse(0)
  lazy val KAFKA_LOG_TOPIC = s"s2graphIn${PHASE}"
  lazy val KAFKA_FAIL_TOPIC = s"s2graphIn${PHASE}Failed"

  // use Keep-Alive
  lazy val USE_KEEP_ALIVE = conf.getBoolean("use.keep.alive").getOrElse(false)

  // is query or write
  lazy val IS_QUERY_SERVER = conf.getBoolean("is.query.server").getOrElse(true)
  lazy val IS_WRITE_SERVER = conf.getBoolean("is.write.server").getOrElse(true)

  val metricRegistry = new com.codahale.metrics.MetricRegistry()
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder  {
  val metricRegistry: MetricRegistry = Config.metricRegistry
  val reporter = Slf4jReporter.forRegistry(metricRegistry)
    .outputTo(LoggerFactory.getLogger(classOf[Instrumented]))
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()
//  val consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
//    .convertRatesTo(TimeUnit.SECONDS)
//    .convertDurationsTo(TimeUnit.MILLISECONDS)
//    .build()

  val stats = new collection.mutable.HashMap[String, Metric]

  /**
   * Edge
   */
  // insert
  def getOrElseUpdateMetric[M <: Metric](key: String)(op: => M)= {
    stats.get(key) match {
      case None =>
        val m = op
        stats += (key -> m)
        m.asInstanceOf[M]
      case Some(m) => m.asInstanceOf[M]
    }
  }
}
