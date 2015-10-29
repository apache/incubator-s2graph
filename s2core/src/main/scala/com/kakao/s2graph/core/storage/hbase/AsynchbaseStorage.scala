package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.storage.{GKeyValue, GStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{DeleteRequest, AtomicIncrementRequest, PutRequest, HBaseRpc}

object AsynchbaseStorage extends GStorable[HBaseRpc, HBaseRpc, HBaseRpc] {
  override def put(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }


  override def increment(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)) }


  override def delete(kvs: Seq[GKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv =>
      if (kv.qualifier == null) new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp)
      else new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp)
    }

  override def fetch(): Seq[GKeyValue] = Nil
}

case class AsynchbaseStorage {

}
