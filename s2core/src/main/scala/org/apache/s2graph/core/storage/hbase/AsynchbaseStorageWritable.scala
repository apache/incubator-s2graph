package org.apache.s2graph.core.storage.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.storage.{IncrementResponse, MutateResponse, SKeyValue, StorageWritable}
import org.apache.s2graph.core.utils.{Extensions, logger}
import org.hbase.async.{AtomicIncrementRequest, DeleteRequest, HBaseClient, PutRequest}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseStorageWritable(val client: HBaseClient,
                                val clientWithFlush: HBaseClient) extends StorageWritable {
  import Extensions.DeferOps

  private def client(withWait: Boolean): HBaseClient = if (withWait) clientWithFlush else client
  /**
    * decide how to store given key values Seq[SKeyValue] into storage using storage's client.
    * note that this should be return true on all success.
    * we assumes that each storage implementation has client as member variable.
    *
    * @param cluster  : where this key values should be stored.
    * @param kvs      : sequence of SKeyValue that need to be stored in storage.
    * @param withWait : flag to control wait ack from storage.
    *                 note that in AsynchbaseStorage(which support asynchronous operations), even with true,
    *                 it never block thread, but rather submit work and notified by event loop when storage send ack back.
    * @return ack message from storage.
    */
  override def writeToStorage(cluster: String, kvs: Seq[SKeyValue], withWait: Boolean)(implicit ec: ExecutionContext) = {
    if (kvs.isEmpty) Future.successful(MutateResponse.Success)
    else {
      val _client = client(withWait)
      val (increments, putAndDeletes) = kvs.partition(_.operation == SKeyValue.Increment)

      /* Asynchbase IncrementRequest does not implement HasQualifiers */
      val incrementsFutures = increments.map { kv =>
        val countVal = Bytes.toLong(kv.value)
        val request = new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, countVal)
        val fallbackFn: (Exception => MutateResponse) = { ex =>
          logger.error(s"mutation failed. $request", ex)
          new IncrementResponse(false, -1L, -1L)
        }
        val future = _client.bufferAtomicIncrement(request).mapWithFallback(0L)(fallbackFn) { resultCount: java.lang.Long =>
          new IncrementResponse(true, resultCount.longValue(), countVal)
        }.toFuture(MutateResponse.IncrementFailure)

        if (withWait) future else Future.successful(MutateResponse.IncrementSuccess)
      }

      /* PutRequest and DeleteRequest accept byte[][] qualifiers/values. */
      val othersFutures = putAndDeletes.groupBy { kv =>
        (kv.table.toSeq, kv.row.toSeq, kv.cf.toSeq, kv.operation, kv.timestamp)
      }.map { case ((table, row, cf, operation, timestamp), groupedKeyValues) =>

        val durability = groupedKeyValues.head.durability
        val qualifiers = new ArrayBuffer[Array[Byte]]()
        val values = new ArrayBuffer[Array[Byte]]()

        groupedKeyValues.foreach { kv =>
          if (kv.qualifier != null) qualifiers += kv.qualifier
          if (kv.value != null) values += kv.value
        }
        val defer = operation match {
          case SKeyValue.Put =>
            val put = new PutRequest(table.toArray, row.toArray, cf.toArray, qualifiers.toArray, values.toArray, timestamp)
            put.setDurable(durability)
            _client.put(put)
          case SKeyValue.Delete =>
            val delete =
              if (qualifiers.isEmpty)
                new DeleteRequest(table.toArray, row.toArray, cf.toArray, timestamp)
              else
                new DeleteRequest(table.toArray, row.toArray, cf.toArray, qualifiers.toArray, timestamp)
            delete.setDurable(durability)
            _client.delete(delete)
        }
        if (withWait) {
          defer.toFuture(new AnyRef()).map(_ => MutateResponse.Success).recover { case ex: Exception =>
            groupedKeyValues.foreach { kv => logger.error(s"mutation failed. $kv", ex) }
            MutateResponse.Failure
          }
        } else Future.successful(MutateResponse.Success)
      }
      for {
        incrementRets <- Future.sequence(incrementsFutures)
        otherRets <- Future.sequence(othersFutures)
      } yield new MutateResponse(isSuccess = (incrementRets ++ otherRets).forall(_.isSuccess))
    }
  }

  /**
    * write requestKeyValue into storage if the current value in storage that is stored matches.
    * note that we only use SnapshotEdge as place for lock, so this method only change SnapshotEdge.
    *
    * Most important thing is this have to be 'atomic' operation.
    * When this operation is mutating requestKeyValue's snapshotEdge, then other thread need to be
    * either blocked or failed on write-write conflict case.
    *
    * Also while this method is still running, then fetchSnapshotEdgeKeyValues should be synchronized to
    * prevent wrong data for read.
    *
    * Best is use storage's concurrency control(either pessimistic or optimistic) such as transaction,
    * compareAndSet to synchronize.
    *
    * for example, AsynchbaseStorage use HBase's CheckAndSet atomic operation to guarantee 'atomicity'.
    * for storage that does not support concurrency control, then storage implementation
    * itself can maintain manual locks that synchronize read(fetchSnapshotEdgeKeyValues)
    * and write(writeLock).
    *
    * @param rpc
    * @param expectedOpt
    * @return
    */
  override def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue])(implicit ec: ExecutionContext): Future[MutateResponse] = {
    val put = new PutRequest(rpc.table, rpc.row, rpc.cf, rpc.qualifier, rpc.value, rpc.timestamp)
    val expected = expectedOpt.map(_.value).getOrElse(Array.empty)
    client(withWait = true).compareAndSet(put, expected).map(true.booleanValue())(ret => ret.booleanValue()).toFuture(true)
      .map(r => new MutateResponse(isSuccess = r))
  }
}
