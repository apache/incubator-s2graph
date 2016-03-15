package org.apache.s2graph.core

trait GraphElement {
  def serviceName: String
  def ts: Long
  def isAsync: Boolean
  def queueKey: String
  def queuePartitionKey: String
  def toLogString(): String
}
