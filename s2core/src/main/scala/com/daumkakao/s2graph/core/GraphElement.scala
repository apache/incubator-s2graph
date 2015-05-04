package com.daumkakao.s2graph.core

import org.hbase.async.{HBaseRpc}

trait GraphElement {
  lazy val serviceName: String = ???
  lazy val isAsync: Boolean = ???
  lazy val queueKey: String = ???
  lazy val queuePartitionKey: String = ???
  def buildPutsAll(): List[HBaseRpc]

}