package org.apache.s2graph.core.storage.rocks

import com.typesafe.config.Config
import org.apache.s2graph.core.storage.StorageManagement
import org.rocksdb.RocksDB

class RocksStorageManagement(val config: Config,
                             val vdb: RocksDB,
                             val db: RocksDB) extends StorageManagement {
  val path =RocksStorage.getFilePath(config)


  override def flush(): Unit = {
    vdb.close()
    db.close()
    RocksStorage.dbPool.asMap().remove(path)
  }

  override def createTable(config: Config, tableNameStr: String): Unit = {}

  override def truncateTable(config: Config, tableNameStr: String): Unit = {}

  override def deleteTable(config: Config, tableNameStr: String): Unit = {}

  override def shutdown(): Unit = flush()
}
