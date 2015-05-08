package com.daumkakao.s2graph.rest.config

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Metric, ConsoleReporter, CsvReporter, MetricRegistry}
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import play.api.Logger

import scala.collection.JavaConversions._
import scala.reflect._

object Config {

  // default configuration file name : application.conf

  lazy val phase = if (System.getProperty("phase") == null) {
    Logger.warn(
      s"""
         |Please set environment variable "phase".
         | - System.setProperty("phase", [your stage: alpha|real|sandbox])
         | - or export phase=[your stage: alpha|real|sandbox]
       """.stripMargin)
    "alpha"
  } else System.getProperty("phase")

  lazy val confFileName = phase match {
    case "production" | "real" => "real/conf/application.conf"
    case "production_gasan" | "real_gasan" => "real_gasan/conf/application.conf"
    case "sandbox" => "sandbox/conf/application.conf"
    case "sandbox_gasan" => "sandbox_gasan/conf/application.conf"
    case "alpha" => "alpha/conf/application.conf"
    case "alpha_gasan" => "alpha_gasan/conf/application.conf"
    case "dev" => "dev/conf/application.conf"
    case "query" => "query/conf/application.conf"
    case "query_gasan" => "query_gasan/conf/application.conf"
    case _ => throw new Exception(s"phase: $phase is not supported")
  }

  lazy val confFile = if (System.getProperty("config.file") == null) confFileName else System.getProperty("config.file")

  //  System.setProperty("config.file", confFileName)
  //  Logger.info(s"start application with $phase")

  /**
   * User submitted configuration ( this config key/values will be used if submitted )
   */
  var userConf: Option[Map[String, Object]] = None

  //  Logger.info(s"[config.file] name : $confFile")
  lazy val conf = confFile match {
    case f if f == "application.conf" => ConfigFactory.load()
    case f => {
      var cf = ConfigFactory.load()
      val newCfEntries = ConfigFactory.load(confFile).entrySet()
      for (entry <- newCfEntries) {
        cf = cf.withValue(entry.getKey, entry.getValue)
      }
      // If user defined cnofigiguration was set
      userConf match {
        case Some(c) =>
          val userEntries = c.entrySet()
          for (entry <- userEntries) {
            cf = cf.withValue(entry.getKey, ConfigValueFactory.fromAnyRef(entry.getValue))
            Logger.debug(s"[User config] ${entry.getKey} : ${entry.getValue}")
          }
        case _ => //ignore
      }
      cf
    }
  }

  /**
   * Set configuration for application
   *  - You have to write setConfig code at main( or bootstrap) entry point
   *  - e.g.
   *
   *         Config.setConfig(Map[String, Object](
   *           "k1" -> Int.box(1),
   *           "kb" -> Boolean.box(false),
   *           "k2" -> "str"
   *         ))
   *
   * @param kvs Config map key/values
   */
  def setConfig(kvs: Map[String, Object]) = {
    if (kvs != Nil && kvs != null) {
      userConf = Some(kvs)
    } else {
      Logger.error("You have to pass Config instance parameter.")
    }
  }

  private[Config] def getOrElse[T: ClassTag](key: String, default: T): T = {
    val ret = if (conf.hasPath(key)) (default match {
      case _: String => conf.getString(key)
      case _: Int | _: Integer => conf.getInt(key)
      case _: Float | _: Double => conf.getDouble(key)
      case _: Boolean => conf.getBoolean(key)
      case _ => default
    }).asInstanceOf[T]
    else default
    Logger.info(s"Config: $key => $ret")
    println(s"Config: $key => $ret")
    ret
  }


  // CACHE
  lazy val CACHE_TTL_SECONDS = getOrElse("cache.ttl.seconds", 600)
  lazy val CACHE_MAX_SIZE = getOrElse("cache.max.size", 10000)

  //KAFKA
  lazy val KAFKA_METADATA_BROKER_LIST = getOrElse("kafka.metadata.broker.list", "localhost")
  lazy val KAFKA_PRODUCER_POOL_SIZE = getOrElse("kafka.producer.pool.size", 0)
  lazy val KAFKA_LOG_TOPIC = s"s2graphIn${phase}"
  lazy val KAFKA_FAIL_TOPIC = s"s2graphIn${phase}Failed"

  // use Keep-Alive
  lazy val USE_KEEP_ALIVE = getOrElse("use.keep.alive", false)

  // is query or write
  lazy val IS_QUERY_SERVER = getOrElse("is.query.server", true)
  lazy val IS_WRITE_SERVER = getOrElse("is.write.server", true)

  val metricRegistry = new com.codahale.metrics.MetricRegistry()
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder  {
  val metricRegistry: MetricRegistry = Config.metricRegistry
  val consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()

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
