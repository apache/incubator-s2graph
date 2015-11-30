package com.kakao.s2graph.core.mysqls

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scalikejdbc._

/**
  * Created by hsleep on 2015. 11. 30..
  */
class ExperimentSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val Ttl = 2
  override def beforeAll(): Unit = {
    /*
    maxSize = config.getInt("cache.max.size")
    ttl = config.getInt("cache.ttl.seconds")
     */
    val props = new Properties()
    props.setProperty("cache.ttl.seconds", Ttl.toString)
    Model.apply(ConfigFactory.load(ConfigFactory.parseProperties(props)))
    /*
CREATE TABLE `experiments` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `service_id` integer NOT NULL,
  `service_name` varchar(128) NOT NULL,
  `name` varchar(64) NOT NULL,
  `description` varchar(255) NOT NULL,
  `experiment_type` varchar(8) NOT NULL DEFAULT 'u',
  `total_modular` int NOT NULL DEFAULT 100,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_service_id_name` (`service_id`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `buckets` (
  `id` integer NOT NULL AUTO_INCREMENT,
  `experiment_id` integer NOT NULL,
  `uuid_mods` varchar(64) NOT NULL,
  `traffic_ratios` varchar(64) NOT NULL,
  `http_verb` varchar(8) NOT NULL,
  `api_path` text NOT NULL,
  `uuid_key` varchar(128),
  `uuid_placeholder` varchar(64),
  `request_body` text NOT NULL,
  `timeout` int NOT NULL DEFAULT 1000,
  `impression_id` varchar(64) NOT NULL,
  `is_graph_query` tinyint NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_impression_id` (`impression_id`),
  INDEX `idx_experiment_id` (`experiment_id`),
  INDEX `idx_impression_id` (`impression_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
     */
    implicit val session = AutoSession
    sql"""DELETE FROM buckets""".update().apply()
    sql"""DELETE FROM experiments""".update().apply()

    val expId = sql"""INSERT INTO experiments(service_id, service_name, `name`, description) VALUES(1, "s1", "exp1", "")""".updateAndReturnGeneratedKey().apply()
    sql"""INSERT INTO
           buckets(experiment_id, modular, http_verb, api_path, request_body, impression_id)
           VALUES($expId, "1~100", "POST", "/a/b/c", "None", "imp1")""".update().apply()
  }

  "Experiment" should "find bucket list" in {
    Experiment.findBy(1, "exp1") should not be empty

    Experiment.findBy(1, "exp1").foreach { exp =>
      val bucket = exp.buckets.head
      bucket.impressionId should equal("imp1")
    }
  }

  it should "update bucket list after cache ttl time" in {
     Experiment.findBy(1, "exp1").foreach { exp =>
      val bucket = exp.buckets.head
      bucket.impressionId should equal("imp1")

      implicit val session = AutoSession

      sql"""UPDATE buckets SET impression_id = "imp2" WHERE id = ${bucket.id}""".update().apply()
    }

    // sleep ttl time
    Thread.sleep((Ttl + 1) * 1000)

    // update experiment and bucket
    Experiment.findBy(1, "exp1").foreach(exp => exp.buckets)

    // wait for cache updating
    Thread.sleep(1 * 1000)

    // should be updated
    Experiment.findBy(1, "exp1").foreach { exp =>
      exp.buckets.head.impressionId should equal("imp2")
    }
  }
}
