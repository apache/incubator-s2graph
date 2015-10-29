package com.kakao.s2graph.core.storage

trait GKeyValue {
  self =>

  val table: Array[Byte]

  val row: Array[Byte]

  val cf: Array[Byte]

  val qualifier: Array[Byte]

  val value: Array[Byte]

  val timestamp: Long

  type A = Array[Byte]

  def copy(_table: A = table,
           _row: A = row,
           _cf: A = cf,
           _qualifier: A = qualifier,
           _value: A = value,
           _timestamp: Long = timestamp) = new GKeyValue {
    override val table: Array[Byte] = _table
    override val cf: Array[Byte] = _cf
    override val value: Array[Byte] = _value
    override val qualifier: Array[Byte] = _qualifier
    override val timestamp: Long = _timestamp
    override val row: Array[Byte] = _row
  }

  override def toString(): String = {
    Map("table" -> table.toList,
      "row" -> row.toList,
      "cf" -> cf.toList,
      "qualifier" -> qualifier.toList,
      "value" -> value.toList,
      "timestamp" -> timestamp).mkString(", ")
  }
}