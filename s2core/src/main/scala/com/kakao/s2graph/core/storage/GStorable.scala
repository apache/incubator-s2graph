package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future


trait GStorable[I, D, C] {

  def put(kvs: Seq[GKeyValue]): Seq[I]

  def delete(kvs: Seq[GKeyValue]): Seq[D]

  def increment(kvs: Seq[GKeyValue]): Seq[C]

  def fetch(): Seq[GKeyValue]

}

