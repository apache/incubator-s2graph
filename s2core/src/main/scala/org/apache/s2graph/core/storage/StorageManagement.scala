package org.apache.s2graph.core.storage

import com.typesafe.config.Config

trait StorageManagement {
  /**
    * this method need to be called when client shutdown. this is responsible to cleanUp the resources
    * such as client into storage.
    */
  def flush(): Unit

  /**
    * create table on storage.
    * if storage implementation does not support namespace or table, then there is nothing to be done
    * @param config
    */
  def createTable(config: Config, tableNameStr: String): Unit
  /**
    *
    * @param config
    * @param tableNameStr
    */
  def truncateTable(config: Config, tableNameStr: String): Unit
  /**
    *
    * @param config
    * @param tableNameStr
    */
  def deleteTable(config: Config, tableNameStr: String): Unit

  /**
    *
    */
  def shutdown(): Unit
}
