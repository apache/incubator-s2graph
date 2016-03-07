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

package s2.helper

import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Durability}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.slf4j.LoggerFactory
import redis.clients.jedis.ScanParams

import scala.collection.JavaConversions._
import scala.util.Random

/**
*  Created by hsleep(honeysleep@gmail.com) on 15. 3. 30..
*/
class Management(config: Config) {
  val withRedis = new HashShardingJedis(config)

  val log = LoggerFactory.getLogger(this.getClass)
  
  def describe(zkAddr: String, tableName: String) = {
    val admin = getAdmin(zkAddr)
    val table = admin.getTableDescriptor(TableName.valueOf(tableName))

    table.getColumnFamilies.foreach { cf =>
      println(s"columnFamily: ${cf.getNameAsString}")
      cf.getValues.foreach { case (k, v) =>
        println(s"${Bytes.toString(k.get())} ${Bytes.toString(v.get())}")
      }
    }
  }
  
  def setTTL(zkAddr: String, tableName: String, cfName: String, ttl: Int) = {
    val admin = getAdmin(zkAddr)
    val tableNameObj = TableName.valueOf(tableName)
    val table = admin.getTableDescriptor(tableNameObj)

    val cf = table.getFamily(cfName.getBytes)
    cf.setTimeToLive(ttl)

    admin.modifyColumn(tableNameObj, cf)
  }

  def getAdmin(zkAddr: String): Admin = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkAddr)
    val conn = ConnectionFactory.createConnection(conf)
    conn.getAdmin
  }

  def tableExists(zkAddr: String, tableName: String): Boolean = {
    getAdmin(zkAddr).tableExists(TableName.valueOf(tableName))
  }

  def createTable(zkAddr: String, tableName: String, cfs: List[String], regionMultiplier: Int) = {
    log.info(s"create table: $tableName on $zkAddr, $cfs, $regionMultiplier")
    val admin = getAdmin(zkAddr)
    val regionCount = admin.getClusterStatus.getServersSize * regionMultiplier
    try {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      desc.setDurability(Durability.ASYNC_WAL)
      for (cf <- cfs) {
        val columnDesc = new HColumnDescriptor(cf)
          .setCompressionType(Compression.Algorithm.LZ4)
          .setBloomFilterType(BloomType.ROW)
          .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
          .setMaxVersions(1)
          .setMinVersions(0)
          .setBlocksize(32768)
          .setBlockCacheEnabled(true)
        desc.addFamily(columnDesc)
      }

      if (regionCount <= 1) admin.createTable(desc)
      else admin.createTable(desc, getStartKey(regionCount), getEndKey(regionCount), regionCount)
    } catch {
      case e: Throwable =>
        log.error(s"$zkAddr, $tableName failed with $e", e)
        throw e
    }
  }

  // we only use murmur hash to distribute row key.
  private def getStartKey(regionCount: Int) = {
    Bytes.toBytes(Int.MaxValue / regionCount)
  }

  private def getEndKey(regionCount: Int) = {
    Bytes.toBytes(Int.MaxValue / regionCount * (regionCount - 1))
  }

  case class RedisScanIterator(scanParams: ScanParams = new ScanParams().count(100)) extends Iterator[String] {
    val nextCursorId: collection.mutable.Map[Int, String] = collection.mutable.Map.empty[Int, String]
    var innerIterator: Iterator[String] = _

    for {
      i <- 0 until withRedis.jedisPoolSize
    } {
      nextCursorId.put(i, "0")
    }

    def callScan(): Unit = {
      if (nextCursorId.nonEmpty) {
//        println(s"callScan: idx: $nextIdx, cursor: $nextCursorId")
        val idx = Random.shuffle(nextCursorId.keys).head
        val cursorId = nextCursorId(idx)
        val pool = withRedis.getJedisPool(idx)
        val conn = pool.getResource
        try {
          val result = conn.scan(cursorId, scanParams)
          result.getStringCursor match {
            case "0" =>
              log.debug(s"end scan: idx: $idx, cursor: $cursorId")
              nextCursorId.remove(idx)
            case x: String =>
              nextCursorId.put(idx, x)
          }
          innerIterator = result.getResult.toIterator
        } finally {
          pool.returnResource(conn)
        }
      }
      else {
        innerIterator = List.empty[String].toIterator
      }
    }

    // initialize
    callScan()

    override def hasNext: Boolean = {
      innerIterator.hasNext match {
        case true =>
          true
        case false =>
          callScan()
          innerIterator.hasNext
      }
    }

    override def next(): String = innerIterator.next()
  }
}
