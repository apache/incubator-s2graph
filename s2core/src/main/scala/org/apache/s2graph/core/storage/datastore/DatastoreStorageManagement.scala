package org.apache.s2graph.core.storage.datastore

import com.spotify.asyncdatastoreclient.Datastore
import com.typesafe.config.Config
import org.apache.s2graph.core.storage.StorageManagement

class DatastoreStorageManagement(datastore: Datastore) extends StorageManagement {
  override def flush(): Unit = {
    datastore.close()
  }

  /**
    * create table on storage.
    * if storage implementation does not support namespace or table, then there is nothing to be done
    *
    * @param config
    */
  override def createTable(config: Config, tableNameStr: String): Unit = {
    // do nothing.
  }

  /**
    *
    * @param config
    * @param tableNameStr
    */
  override def truncateTable(config: Config, tableNameStr: String): Unit = {
    // do nothing.
  }

  /**
    *
    * @param config
    * @param tableNameStr
    */
  override def deleteTable(config: Config, tableNameStr: String): Unit = {
    // do nothing.
  }

  /**
    *
    */
  override def shutdown(): Unit = {
    flush()
  }
}
