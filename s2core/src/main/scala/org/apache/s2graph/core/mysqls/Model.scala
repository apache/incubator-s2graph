/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.mysqls

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.JSONParser
import org.apache.s2graph.core.utils.{SafeUpdateCache, logger}
import play.api.libs.json.{JsObject, JsValue, Json}
import scalikejdbc._

import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.language.{higherKinds, implicitConversions}
import scala.util.{Failure, Success, Try}

object Model {
  var maxSize = 10000
  var ttl = 60
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)
  val useUTF8Encoding = "?useUnicode=true&characterEncoding=utf8"

  private val ModelReferenceCount = new AtomicLong(0L)

  def apply(config: Config) = {
    maxSize = config.getInt("cache.max.size")
    ttl = config.getInt("cache.ttl.seconds")
    Class.forName(config.getString("db.default.driver"))

    val settings = ConnectionPoolSettings(
      initialSize = 10,
      maxSize = 10,
      connectionTimeoutMillis = 30000L,
      validationQuery = "select 1;")

    ConnectionPool.singleton(
      config.getString("db.default.url"),
      config.getString("db.default.user"),
      config.getString("db.default.password"),
      settings)

    checkSchema()

    ModelReferenceCount.incrementAndGet()
  }

  def checkSchema(): Unit = {
    withTx { implicit session =>
      sql"""show tables""".map(rs => rs.string(1)).list.apply()
    } match {
      case Success(tables) =>
        if (tables.isEmpty) {
          // this is a very simple migration tool that only supports creating
          // appropriate tables when there are no tables in the database at all.
          // Ideally, it should be improved to a sophisticated migration tool
          // that supports versioning, etc.
          logger.info("Creating tables ...")
          val schema = getClass.getResourceAsStream("schema.sql")
          val lines = Source.fromInputStream(schema, "UTF-8").getLines
          val sources = lines.map(_.split("-- ").head.trim).mkString("\n")
          val statements = sources.split(";\n")
          withTx { implicit session =>
            statements.foreach(sql => session.execute(sql))
          } match {
            case Success(_) =>
              logger.info("Successfully imported schema")
            case Failure(e) =>
              throw new RuntimeException("Error while importing schema", e)
          }
        }
      case Failure(e) =>
        throw new RuntimeException("Could not list tables in the database", e)
    }
  }

  def withTx[T](block: DBSession => T): Try[T] = {
    using(DB(ConnectionPool.borrow())) { conn =>
      Try {
        conn.begin()
        val session = conn.withinTxSession()
        val result = block(session)

        conn.commit()

        result
      } recoverWith {
        case e: Exception =>
          conn.rollbackIfActive()
          Failure(e)
      }
    }
  }

  def shutdown(modelDataDelete: Boolean = false) =
    if (ModelReferenceCount.decrementAndGet() <= 0) {
      // FIXME: When Model is served by embedded database and deleteData is set, Model deletes
      // the underlying database. Its purpose is clearing runtime footprint when running tests.
      if (modelDataDelete) {
        withTx { implicit session =>
          sql"SHOW TABLES"
              .map(rs => rs.string(1))
              .list
              .apply()
              .map { table => s"TRUNCATE TABLE $table" }
        } match {
          case Success(stmts) =>
            val newStmts = List("SET FOREIGN_KEY_CHECKS = 0") ++ stmts ++ List("SET FOREIGN_KEY_CHECKS = 1")
            withTx { implicit session =>
              newStmts.foreach { stmt =>
                session.execute(stmt)
              }
            } match {
              case Success(_) =>
                logger.info(s"Success to truncate models: $stmts")
              case Failure(e) =>
                throw new IllegalStateException(s"Failed to truncate models", e)
            }
          case Failure(e) =>
            throw new IllegalStateException(s"Failed to list models", e)
        }
      }
      clearCache()
      ConnectionPool.closeAll()
    }

  def loadCache() = {
    Service.findAll()
    ServiceColumn.findAll()
    Label.findAll()
    LabelMeta.findAll()
    LabelIndex.findAll()
    ColumnMeta.findAll()
  }

  def clearCache() = {
    Service.expireAll()
    ServiceColumn.expireAll()
    Label.expireAll()
    LabelMeta.expireAll()
    LabelIndex.expireAll()
    ColumnMeta.expireAll()
  }

  def extraOptions(options: Option[String]): Map[String, JsValue] = options match {
    case None => Map.empty
    case Some(v) =>
      try {
        Json.parse(v).asOpt[JsObject].map { obj => obj.fields.toMap }.getOrElse(Map.empty)
      } catch {
        case e: Exception =>
          logger.error(s"An error occurs while parsing the extra label option", e)
          Map.empty
      }
  }

  def toStorageConfig(options: Map[String, JsValue]): Option[Config] = {
    try {
      options.get("storage").map { jsValue =>
        import scala.collection.JavaConverters._
        val configMap = jsValue.as[JsObject].fieldSet.toMap.map { case (key, value) =>
          key -> JSONParser.jsValueToAny(value).getOrElse(throw new RuntimeException("!!"))
        }
        ConfigFactory.parseMap(configMap.asJava)
      }
    } catch {
      case e: Exception =>
        logger.error(s"toStorageConfig error. use default storage", e)
        None
    }
  }
}

trait Model[V] extends SQLSyntaxSupport[V] {

  import Model._

  implicit val ec: ExecutionContext = Model.ec

  val cName = this.getClass.getSimpleName()
  logger.info(s"LocalCache[$cName]: TTL[$ttl], MaxSize[$maxSize]")

  val optionCache = new SafeUpdateCache[Option[V]](cName, maxSize, ttl)
  val listCache = new SafeUpdateCache[List[V]](cName, maxSize, ttl)

  val withCache = optionCache.withCache _

  val withCaches = listCache.withCache _

  val expireCache = optionCache.invalidate _

  val expireCaches = listCache.invalidate _

  def expireAll() = {
    listCache.invalidateAll()
    optionCache.invalidateAll()
  }

  def putsToCache(kvs: List[(String, V)]) = kvs.foreach {
    case (key, value) => optionCache.put(key, Option(value))
  }

  def putsToCaches(kvs: List[(String, List[V])]) = kvs.foreach {
    case (key, values) => listCache.put(key, values)
  }

  def getAllCacheData() : (List[(String, Option[_])], List[(String, List[_])]) = {
    (optionCache.getAllData(), listCache.getAllData())
  }
}

