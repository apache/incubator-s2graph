package org.apache.s2graph.core.storage.rocks

import java.util.concurrent.locks.ReentrantLock

import com.google.common.cache.{Cache, LoadingCache}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.storage.{MutateResponse, SKeyValue, StorageWritable}
import org.apache.s2graph.core.utils.logger
import org.rocksdb.{RocksDB, RocksDBException, WriteBatch, WriteOptions}

import scala.concurrent.{ExecutionContext, Future}

class RocksStorageWritable(val db: RocksDB,
                           val vdb: RocksDB,
                           val lockMap: LoadingCache[String, ReentrantLock]) extends StorageWritable {

  override def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean)(implicit ec: ExecutionContext) = {
    if (kvs.isEmpty) {
      Future.successful(MutateResponse.Success)
    } else {
      val ret = {
        val (kvsV, kvsE) = kvs.partition(kv => Bytes.equals(kv.cf, SKeyValue.VertexCf))
        val writeBatchV = buildWriteBatch(kvsV)
        val writeBatchE = buildWriteBatch(kvsE)
        val writeOptions = new WriteOptions
        try {
          vdb.write(writeOptions, writeBatchV)
          db.write(writeOptions, writeBatchE)
          true
        } catch {
          case e: Exception =>
            logger.error(s"writeAsyncSimple failed.", e)
            false
        } finally {
          writeBatchV.close()
          writeBatchE.close()
          writeOptions.close()
        }
      }

      Future.successful(new MutateResponse(ret))
    }
  }


  override def writeLock(requestKeyValue: SKeyValue, expectedOpt: Option[SKeyValue])(implicit ec: ExecutionContext) = {
    def op = {
      val writeOptions = new WriteOptions
      try {
        val fetchedValue = db.get(requestKeyValue.row)
        val innerRet = expectedOpt match {
          case None =>
            if (fetchedValue == null) {

              db.put(writeOptions, requestKeyValue.row, requestKeyValue.value)
              true
            } else {
              false
            }
          case Some(kv) =>
            if (fetchedValue == null) {
              false
            } else {
              if (Bytes.compareTo(fetchedValue, kv.value) == 0) {
                db.put(writeOptions, requestKeyValue.row, requestKeyValue.value)
                true
              } else {
                false
              }
            }
        }

        Future.successful(new MutateResponse(innerRet))
      } catch {
        case e: RocksDBException =>
          logger.error(s"Write lock failed", e)
          Future.successful(MutateResponse.Failure)
      } finally {
        writeOptions.close()
      }
    }

    withLock(requestKeyValue.row)(op)
  }

  private def buildWriteBatch(kvs: Seq[SKeyValue]): WriteBatch = {
    val writeBatch = new WriteBatch()
    kvs.foreach { kv =>
      kv.operation match {
        case SKeyValue.Put => writeBatch.put(kv.row, kv.value)
        case SKeyValue.Delete => writeBatch.remove(kv.row)
        case SKeyValue.Increment => writeBatch.merge(kv.row, kv.value)
        case _ => throw new RuntimeException(s"not supported rpc operation. ${kv.operation}")
      }
    }
    writeBatch
  }

  private def withLock[A](key: Array[Byte])(op: => A): A = {
    val lockKey = Bytes.toString(key)
    val lock = lockMap.get(lockKey)

    try {
      lock.lock
      op
    } finally {
      lock.unlock()
    }
  }
}
