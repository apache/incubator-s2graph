/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */

import org.apache.s2graph.core.GraphExceptions.MaxPropSizeReachedException
import org.apache.s2graph.core.{GraphExceptions, JSONParser}
import play.api.libs.json.Json
import scalikejdbc._

import scala.util.Try

object LabelMeta extends Model[LabelMeta] {

  /** dummy sequences */

  val fromSeq = (-4).toByte
  val toSeq = (-5).toByte
  val lastOpSeq = (-3).toByte
  val lastDeletedAtSeq = (-2).toByte
  val timestampSeq = (0).toByte
  val labelSeq = (-6).toByte
  val directionSeq = -7.toByte
  val fromHashSeq = -8.toByte

  val countSeq = (Byte.MaxValue - 2).toByte
  val degreeSeq = (Byte.MaxValue - 1).toByte
  val maxValue = Byte.MaxValue
  val emptySeq = Byte.MaxValue

  /** reserved sequences */
  //  val deleted = LabelMeta(id = Some(lastDeletedAt), labelId = lastDeletedAt, name = "lastDeletedAt",
  //    seq = lastDeletedAt, defaultValue = "", dataType = "long")
  val fromHash = LabelMeta(id = None, labelId = fromHashSeq, name = "_from_hash",
    seq = fromHashSeq, defaultValue = fromHashSeq.toString, dataType = "long")
  val from = LabelMeta(id = Some(fromSeq), labelId = fromSeq, name = "_from",
    seq = fromSeq, defaultValue = fromSeq.toString, dataType = "string")
  val to = LabelMeta(id = Some(toSeq), labelId = toSeq, name = "_to",
    seq = toSeq, defaultValue = toSeq.toString, dataType = "string")
  val timestamp = LabelMeta(id = Some(-1), labelId = -1, name = "_timestamp",
    seq = timestampSeq, defaultValue = "0", dataType = "long")
  val degree = LabelMeta(id = Some(-1), labelId = -1, name = "_degree",
    seq = degreeSeq, defaultValue = "0", dataType = "long")
  val count = LabelMeta(id = Some(-1), labelId = -1, name = "_count",
    seq = countSeq, defaultValue = "-1", dataType = "long")
  val lastDeletedAt = LabelMeta(id = Some(-1), labelId = -1, name = "_lastDeletedAt",
    seq = lastDeletedAtSeq, defaultValue = "-1", dataType = "long")
  val label = LabelMeta(id = Some(-1), labelId = -1, name = "label",
    seq = labelSeq, defaultValue = "", dataType = "string")
  val direction = LabelMeta(id = Some(-1), labelId = -1, name = "direction",
    seq = directionSeq, defaultValue = "out", dataType = "string")
  val empty = LabelMeta(id = Some(-1), labelId = -1, name = "_empty",
    seq = emptySeq, defaultValue = "-1", dataType = "long")

  // Each reserved column(_timestamp, timestamp) has same seq number, starts with '_' has high priority
  val reservedMetas = List(empty, label, direction, lastDeletedAt, from, fromHash, to, degree, timestamp, count).flatMap { lm => List(lm, lm.copy(name = lm.name.drop(1))) }.reverse
  val reservedMetasInner = List(empty, label, direction, lastDeletedAt, from, fromHash, to, degree, timestamp, count)
  val reservedMetaNamesSet = reservedMetasInner.map(_.name).toSet

  val defaultRequiredMetaNames = Set("from", "_from", "to", "_to", "_from_hash", "label", "direction", "timestamp", "_timestamp")

  def apply(rs: WrappedResultSet): LabelMeta = {
    LabelMeta(Some(rs.int("id")), rs.int("label_id"), rs.string("name"), rs.byte("seq"),
      rs.string("default_value"), rs.string("data_type").toLowerCase, rs.boolean("store_in_global_index"))
  }

  /** Note: DegreeSeq should not be included in serializer/deserializer.
    * only 0 <= seq <= CountSeq(Int.MaxValue - 2), not DegreeSet(Int.MaxValue - 1) should be
    * included in actual bytes in storage.
    * */
  def isValidSeq(seq: Byte): Boolean = seq >= 0 && seq <= countSeq // || seq == fromHashSeq

  def isValidSeqForAdmin(seq: Byte): Boolean = seq > 0 && seq < countSeq // || seq == fromHashSeq

  def findById(id: Int)(implicit session: DBSession = AutoSession): LabelMeta = {
    val cacheKey = "id=" + id

    withCache(cacheKey) {
      sql"""select * from label_metas where id = ${id}""".map { rs => LabelMeta(rs) }.single.apply
    }.get
  }

  def findAllByLabelId(labelId: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession): List[LabelMeta] = {
    val cacheKey = "labelId=" + labelId
    lazy val labelMetas = sql"""select *
    		  						from label_metas
    		  						where label_id = ${labelId} order by seq ASC""".map(LabelMeta(_)).list.apply()

    if (useCache) withCaches(cacheKey)(labelMetas)
    else labelMetas
  }

  def findByName(labelId: Int, name: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[LabelMeta] = {
    name match {
      case timestamp.name => Some(timestamp)
      case from.name => Some(from)
      case to.name => Some(to)
      case _ =>
        val cacheKey = "labelId=" + labelId + ":name=" + name
        lazy val labelMeta = sql"""
            select *
            from label_metas where label_id = ${labelId} and name = ${name}"""
          .map { rs => LabelMeta(rs) }.single.apply()

        if (useCache) withCache(cacheKey)(labelMeta)
        else labelMeta
    }
  }

  def insert(labelId: Int, name: String, defaultValue: String, dataType: String, storeInGlobalIndex: Boolean = false)(implicit session: DBSession = AutoSession) = {
    val ls = findAllByLabelId(labelId, useCache = false)
    val seq = ls.size + 1

    if (seq < maxValue) {
      sql"""insert into label_metas(label_id, name, seq, default_value, data_type, store_in_global_index)
    select ${labelId}, ${name}, ${seq}, ${defaultValue}, ${dataType}, ${storeInGlobalIndex}""".updateAndReturnGeneratedKey.apply()
    } else {
      throw MaxPropSizeReachedException("max property size reached")
    }
  }

  def findOrInsert(labelId: Int,
                   name: String,
                   defaultValue: String,
                   dataType: String,
                   storeInGlobalIndex: Boolean = false)(implicit session: DBSession = AutoSession): LabelMeta = {

    findByName(labelId, name) match {
      case Some(c) => c
      case None =>
        insert(labelId, name, defaultValue, dataType, storeInGlobalIndex)
        val cacheKey = "labelId=" + labelId + ":name=" + name
        val cacheKeys = "labelId=" + labelId
        expireCache(cacheKey)
        expireCaches(cacheKeys)
        findByName(labelId, name, useCache = false).get
    }
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val labelMeta = findById(id)
    val (labelId, name) = (labelMeta.labelId, labelMeta.name)
    sql"""delete from label_metas where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"labelId=$labelId", s"labelId=$labelId:name=$name")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
  }

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from label_metas""".map { rs => LabelMeta(rs) }.list.apply
    putsToCache(ls.map { x =>
      val cacheKey = s"id=${x.id.get}"
      cacheKey -> x
    })
    putsToCache(ls.map { x =>
      val cacheKey = s"labelId=${x.labelId}:name=${x.name}"
      cacheKey -> x
    })
    putsToCache(ls.map { x =>
      val cacheKey = s"labelId=${x.labelId}:seq=${x.seq}"
      cacheKey -> x
    })

    putsToCaches(ls.groupBy(x => x.labelId).map { case (labelId, ls) =>
      val cacheKey = s"labelId=${labelId}"
      cacheKey -> ls
    }.toList)

    ls
  }

  def updateStoreInGlobalIndex(id: Int, storeInGlobalIndex: Boolean)(implicit session: DBSession = AutoSession): Try[Long] = Try {
    sql"""
          update label_metas set store_in_global_index = ${storeInGlobalIndex} where id = ${id}
       """.updateAndReturnGeneratedKey.apply()
  }
}

case class LabelMeta(id: Option[Int],
                     labelId: Int,
                     name: String,
                     seq: Byte,
                     defaultValue: String,
                     dataType: String,
                     storeInGlobalIndex: Boolean = false) {
  lazy val toJson = Json.obj("name" -> name, "defaultValue" -> defaultValue, "dataType" -> dataType)
  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[LabelMeta]) false
    else {
      val o = other.asInstanceOf[LabelMeta]
//      labelId == o.labelId &&
        seq == o.seq
    }
  }
  override def hashCode(): Int = seq.toInt
//    (labelId, seq).hashCode()
}
