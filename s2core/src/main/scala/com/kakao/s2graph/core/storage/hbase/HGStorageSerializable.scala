package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.storage.GKeyValue

trait HGStorageSerializable[E] {
  def toKeyValues: Seq[GKeyValue]
}