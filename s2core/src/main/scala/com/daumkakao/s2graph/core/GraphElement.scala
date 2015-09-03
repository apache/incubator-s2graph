package com.daumkakao.s2graph.core

import org.hbase.async.{HBaseRpc}

trait GraphElement {
  def serviceName: String
  def ts: Long
  def isAsync: Boolean
  def queueKey: String
  def queuePartitionKey: String
  def buildPutsAll(): List[HBaseRpc]
  def toLogString(): String
}