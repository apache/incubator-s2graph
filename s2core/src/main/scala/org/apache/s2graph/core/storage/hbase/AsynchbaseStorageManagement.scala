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

package org.apache.s2graph.core.storage.hbase

import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Durability}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.s2graph.core.storage.StorageManagement
import org.apache.s2graph.core.utils.{Extensions, logger}
import org.hbase.async.HBaseClient

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, duration}
import scala.util.Try
import scala.util.control.NonFatal

object AsynchbaseStorageManagement {
  /* Secure cluster */
  val SecurityAuthEnabled = "hbase.security.auth.enable"
  val Jaas = "java.security.auth.login.config"
  val Krb5Conf = "java.security.krb5.conf"
  val Realm = "realm"
  val Principal = "principal"
  val Keytab = "keytab"
  val HadoopAuthentication = "hadoop.security.authentication"
  val HBaseAuthentication = "hbase.security.authentication"
  val MasterKerberosPrincipal = "hbase.master.kerberos.principal"
  val RegionServerKerberosPrincipal = "hbase.regionserver.kerberos.principal"


  val DefaultCreateTableOptions = Map(
    "hbase.zookeeper.quorum" -> "localhost"
  )
}

class AsynchbaseStorageManagement(val config: Config, val clients: Seq[HBaseClient]) extends StorageManagement {
  import org.apache.s2graph.core.Management._
  import AsynchbaseStorageManagement._
  import Extensions.DeferOps

  /**
    * Asynchbase client setup.
    * note that we need two client, one for bulk(withWait=false) and another for withWait=true
    */
  private val clientFlushInterval = config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort

  /**
    * this method need to be called when client shutdown. this is responsible to cleanUp the resources
    * such as client into storage.
    */
  override def flush(): Unit = clients.foreach { client =>
    implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

    val timeout = Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS)
    Await.result(client.flush().toFuture(new AnyRef), timeout)
  }

  def getOption[T](config: Config, key: String): Option[T] = {
    import scala.util._
    Try { config.getAnyRef(key).asInstanceOf[T] }.toOption
  }
  /**
    * create table on storage.
    * if storage implementation does not support namespace or table, then there is nothing to be done
    *
    * @param config
    */
  override def createTable(config: Config, tableNameStr: String): Unit = {
    val zkAddr = config.getString(ZookeeperQuorum)

    withAdmin(config) { admin =>
      val regionMultiplier = getOption[Int](config, RegionMultiplier).getOrElse(0)
      val regionCount = getOption[Int](config, TotalRegionCount).getOrElse(admin.getClusterStatus.getServersSize * regionMultiplier)
      val cfs = getOption[Seq[String]](config, ColumnFamilies).getOrElse(DefaultColumnFamilies)
      val compressionAlgorithm = getOption[String](config, CompressionAlgorithm).getOrElse(DefaultCompressionAlgorithm)
      val ttl = getOption[Int](config, Ttl)
      val replicationScoreOpt = getOption[Int](config, ReplicationScope)

      val tableName = TableName.valueOf(tableNameStr)
      try {
        if (!admin.tableExists(tableName)) {
          val desc = new HTableDescriptor(tableName)
          desc.setDurability(Durability.ASYNC_WAL)
          for (cf <- cfs) {
            val columnDesc = new HColumnDescriptor(cf)
              .setCompressionType(Algorithm.valueOf(compressionAlgorithm.toUpperCase))
              .setBloomFilterType(BloomType.ROW)
              .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
              .setMaxVersions(1)
              .setTimeToLive(2147483647)
              .setMinVersions(0)
              .setBlocksize(32768)
              .setBlockCacheEnabled(true)
              // FIXME: For test!!
              .setInMemory(true)

            ttl.foreach(columnDesc.setTimeToLive(_))
            replicationScoreOpt.foreach(columnDesc.setScope(_))

            desc.addFamily(columnDesc)
          }

          if (regionCount <= 1) admin.createTable(desc)
          else admin.createTable(desc, getStartKey(regionCount), getEndKey(regionCount), regionCount)
        } else {
          logger.info(s"$zkAddr, $tableName, $cfs already exist.")
        }
      } catch {
        case e: Throwable =>
          logger.error(s"$zkAddr, $tableName failed with $e", e)
          throw e
      }
    }
  }

  /**
    *
    * @param config
    * @param tableNameStr
    */
  override def truncateTable(config: Config, tableNameStr: String): Unit = {
    withAdmin(config) { admin =>
      val tableName = TableName.valueOf(tableNameStr)
      if (!Try(admin.tableExists(tableName)).getOrElse(false)) {
        logger.info(s"No table to truncate ${tableNameStr}")
        return
      }

      Try(admin.isTableDisabled(tableName)).map {
        case true =>
          logger.info(s"${tableNameStr} is already disabled.")

        case false =>
          logger.info(s"Before disabling to trucate ${tableNameStr}")
          Try(admin.disableTable(tableName)).recover {
            case NonFatal(e) =>
              logger.info(s"Failed to disable ${tableNameStr}: ${e}")
          }
          logger.info(s"After disabling to trucate ${tableNameStr}")
      }

      logger.info(s"Before truncating ${tableNameStr}")
      Try(admin.truncateTable(tableName, true)).recover {
        case NonFatal(e) =>
          logger.info(s"Failed to truncate ${tableNameStr}: ${e}")
      }
      logger.info(s"After truncating ${tableNameStr}")
      Try(admin.close()).recover {
        case NonFatal(e) =>
          logger.info(s"Failed to close admin ${tableNameStr}: ${e}")
      }
      Try(admin.getConnection.close()).recover {
        case NonFatal(e) =>
          logger.info(s"Failed to close connection ${tableNameStr}: ${e}")
      }
    }
  }

  /**
    *
    * @param config
    * @param tableNameStr
    */
  override def deleteTable(config: Config, tableNameStr: String): Unit = {
    withAdmin(config) { admin =>
      val tableName = TableName.valueOf(tableNameStr)
      if (!admin.tableExists(tableName)) {
        return
      }
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName)
      }
      admin.deleteTable(tableName)
    }
  }

  /**
    *
    */
  override def shutdown(): Unit = {
    flush()
    clients.foreach { client =>
      AsynchbaseStorage.shutdown(client)
    }
  }



  private def getSecureClusterAdmin(config: Config) = {
    val zkAddr = config.getString(ZookeeperQuorum)
    val realm = config.getString(Realm)
    val principal = config.getString(Principal)
    val keytab = config.getString(Keytab)

    System.setProperty(Jaas, config.getString(Jaas))
    System.setProperty(Krb5Conf, config.getString(Krb5Conf))


    val conf = new Configuration(true)
    val hConf = HBaseConfiguration.create(conf)

    hConf.set(ZookeeperQuorum, zkAddr)

    hConf.set(HadoopAuthentication, "Kerberos")
    hConf.set(HBaseAuthentication, "Kerberos")
    hConf.set(MasterKerberosPrincipal, "hbase/_HOST@" + realm)
    hConf.set(RegionServerKerberosPrincipal, "hbase/_HOST@" + realm)

    System.out.println("Connecting secure cluster, using keytab\n")
    UserGroupInformation.setConfiguration(hConf)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)
    val currentUser = UserGroupInformation.getCurrentUser()
    System.out.println("current user : " + currentUser + "\n")

    // get table list
    val conn = ConnectionFactory.createConnection(hConf)
    conn.getAdmin
  }

  private def withAdmin(config: Config)(op: Admin => Unit): Unit = {
    val admin = getAdmin(config)
    try {
      op(admin)
    } finally {
      admin.close()
      admin.getConnection.close()
    }
  }
  /**
    * following configuration need to come together to use secured hbase cluster.
    * 1. set hbase.security.auth.enable = true
    * 2. set file path to jaas file java.security.auth.login.config
    * 3. set file path to kerberos file java.security.krb5.conf
    * 4. set realm
    * 5. set principal
    * 6. set file path to keytab
    */
  private def getAdmin(config: Config) = {
    if (config.hasPath(SecurityAuthEnabled) && config.getBoolean(SecurityAuthEnabled)) {
      getSecureClusterAdmin(config)
    } else {
      val zkAddr = config.getString(ZookeeperQuorum)
      val conf = HBaseConfiguration.create()
      conf.set(ZookeeperQuorum, zkAddr)
      val conn = ConnectionFactory.createConnection(conf)
      conn.getAdmin
    }
  }

  private def getStartKey(regionCount: Int): Array[Byte] = {
    Bytes.toBytes((Int.MaxValue / regionCount))
  }

  private def getEndKey(regionCount: Int): Array[Byte] = {
    Bytes.toBytes((Int.MaxValue / regionCount * (regionCount - 1)))
  }
}
