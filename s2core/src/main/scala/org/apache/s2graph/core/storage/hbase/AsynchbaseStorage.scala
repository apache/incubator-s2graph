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


import java.util
import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.config.Config
import org.apache.commons.io.FileUtils
import org.apache.s2graph.core._
import org.apache.s2graph.core.storage._
import org.apache.s2graph.core.utils._
import org.hbase.async._
import org.apache.s2graph.core.storage.serde._
import scala.collection.JavaConversions._


object AsynchbaseStorage {
  val vertexCf = Serializable.vertexCf
  val edgeCf = Serializable.edgeCf
  val emptyKVs = new util.ArrayList[KeyValue]()

  AsynchbasePatcher.init()

  def makeClient(config: Config, overrideKv: (String, String)*) = {
    val asyncConfig: org.hbase.async.Config =
      if (config.hasPath("hbase.security.auth.enable") && config.getBoolean("hbase.security.auth.enable")) {
        val krb5Conf = config.getString("java.security.krb5.conf")
        val jaas = config.getString("java.security.auth.login.config")

        System.setProperty("java.security.krb5.conf", krb5Conf)
        System.setProperty("java.security.auth.login.config", jaas)
        new org.hbase.async.Config()
      } else {
        new org.hbase.async.Config()
      }

    for (entry <- config.entrySet() if entry.getKey.contains("hbase")) {
      asyncConfig.overrideConfig(entry.getKey, entry.getValue.unwrapped().toString)
    }

    for ((k, v) <- overrideKv) {
      asyncConfig.overrideConfig(k, v)
    }

    val client = new HBaseClient(asyncConfig)
    logger.info(s"Asynchbase: ${client.getConfig.dumpConfiguration()}")
    client
  }

  def shutdown(client: HBaseClient): Unit = {
    client.shutdown().join()
  }

  case class ScanWithRange(scan: Scanner, offset: Int, limit: Int)

  type AsyncRPC = Either[GetRequest, ScanWithRange]

  def initLocalHBase(config: Config,
                     overwrite: Boolean = true): ExecutorService = {
    import java.io.{File, IOException}
    import java.net.Socket

    lazy val hbaseExecutor = {
      val executor = Executors.newSingleThreadExecutor()

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          executor.shutdown()
        }
      })

      val hbaseAvailable = try {
        val (host, port) = config.getString("hbase.zookeeper.quorum").split(":") match {
          case Array(h, p) => (h, p.toInt)
          case Array(h) => (h, 2181)
        }

        val socket = new Socket(host, port)
        socket.close()
        logger.info(s"HBase is available.")
        true
      } catch {
        case e: IOException =>
          logger.info(s"HBase is not available.")
          false
      }

      if (!hbaseAvailable) {
        // start HBase
        executor.submit(new Runnable {
          override def run(): Unit = {
            logger.info(s"HMaster starting...")
            val ts = System.currentTimeMillis()
            val cwd = new File(".").getAbsolutePath
            if (overwrite) {
              val dataDir = new File(s"$cwd/storage/s2graph")
              FileUtils.deleteDirectory(dataDir)
            }

            System.setProperty("proc_master", "")
            System.setProperty("hbase.log.dir", s"$cwd/storage/s2graph/hbase/")
            System.setProperty("hbase.log.file", s"$cwd/storage/s2graph/hbase.log")
            System.setProperty("hbase.tmp.dir", s"$cwd/storage/s2graph/hbase/")
            System.setProperty("hbase.home.dir", "")
            System.setProperty("hbase.id.str", "s2graph")
            System.setProperty("hbase.root.logger", "INFO,RFA")

            org.apache.hadoop.hbase.master.HMaster.main(Array[String]("start"))
            logger.info(s"HMaster startup finished: ${System.currentTimeMillis() - ts}")
          }
        })
      }

      executor
    }

    hbaseExecutor
  }
}


class AsynchbaseStorage(override val graph: S2Graph,
                        override val config: Config) extends Storage(graph, config) {

  /**
    * since some runtime environment such as spark cluster has issue with guava version, that is used in Asynchbase.
    * to fix version conflict, make this as lazy val for clients that don't require hbase client.
    */
  val client = AsynchbaseStorage.makeClient(config)
  val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  val clients = Seq(client, clientWithFlush)

  override val management: StorageManagement = new AsynchbaseStorageManagement(config, clients)

  override val mutator: StorageWritable = new AsynchbaseStorageWritable(client, clientWithFlush)

  override val serDe: StorageSerDe = new AsynchbaseStorageSerDe(graph)

  override val fetcher: StorageReadable = new AsynchbaseStorageReadable(graph, config, client, serDe, io)

  //  val hbaseExecutor: ExecutorService  =
  //    if (config.getString("hbase.zookeeper.quorum") == "localhost")
  //      AsynchbaseStorage.initLocalHBase(config)
  //    else
  //      null


}
