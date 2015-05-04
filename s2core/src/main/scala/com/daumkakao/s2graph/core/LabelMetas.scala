package com.daumkakao.s2graph.core

import HBaseElement.InnerVal
import play.api.libs.json.{ JsObject, JsValue, Json }
import scalikejdbc._

object LabelMeta extends Model[LabelMeta] with JSONParser {

  /** dummy sequences */

  val fromSeq = -4.toByte
  val toSeq = -5.toByte
  val lastOpSeq = -3.toByte
  val lastDeletedAt = -2.toByte
  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val maxValue = Byte.MaxValue
  val emptyValue = Byte.MaxValue

  /** reserved sequences */
  val from = LabelMeta(id = Some(fromSeq), labelId = fromSeq, name = "_from",
    seq = fromSeq, defaultValue = fromSeq.toString, dataType = "long", usedInIndex = true)
  val to = LabelMeta(id = Some(toSeq), labelId = toSeq, name = "_to",
    seq = toSeq, defaultValue = toSeq.toString, dataType = "long", usedInIndex = true)
  val timestamp = LabelMeta(id = Some(-1), labelId = -1, name = "_timestamp",
    seq = timeStampSeq, defaultValue = "0", dataType = "long", usedInIndex = true)

  val reservedMetas = List(from, to, timestamp)
  val notExistSeqInDB = List(lastOpSeq, lastDeletedAt, countSeq, timeStampSeq, from.seq, to.seq)

  def apply(rs: WrappedResultSet): LabelMeta = {
    LabelMeta(Some(rs.int("id")), rs.int("label_id"), rs.string("name"), rs.byte("seq"),
      rs.string("default_value"), rs.string("data_type"), rs.boolean("used_in_index"))
  }

  def findById(id: Int): LabelMeta = {
    val cacheKey = s"id=$id"
    withCache(cacheKey) {
      sql"""select * from label_metas where id = ${id}""".map { rs => LabelMeta(rs) }.single.apply
    }.get
  }

  def findAllByLabelId(labelId: Int, useCache: Boolean = true): List[LabelMeta] = {
    val cacheKey = s"labelId=$labelId"
    if (useCache) {
      withCaches(cacheKey)(sql"""select *
    		  						from label_metas 
    		  						where label_id = ${labelId} order by seq ASC"""
        .map { rs => LabelMeta(rs) }.list.apply())
    } else {
      sql"""select *
      		from label_metas 
      		where label_id = ${labelId} order by seq ASC"""
        .map { rs => LabelMeta(rs) }.list.apply()
    }

  }

  def findByName(labelIdWithName: (Int, String)): Option[LabelMeta] = {
    val (labelId, name) = labelIdWithName
    findByName(labelId, name)
  }
  def findByName(labelId: Int, name: String, useCache: Boolean = true): Option[LabelMeta] = {
    name match {
      case timestamp.name => Some(timestamp)
      case to.name => Some(to)
      case _ =>
        val cacheKey = s"labelId=$labelId:name=$name"
        if (useCache) {
          withCache(cacheKey)(sql"""
            select *
            from label_metas where label_id = ${labelId} and name = ${name}"""
            .map { rs => LabelMeta(rs) }.single.apply())
        } else {
          sql"""
            select *
            from label_metas where label_id = ${labelId} and name = ${name}"""
            .map { rs => LabelMeta(rs) }.single.apply()
        }

    }

  }
  def insert(labelId: Int, name: String, defaultValue: String, dataType: String, usedInIndex: Boolean) = {
    val ls = findAllByLabelId(labelId, false)
    //    val seq = LabelIndexProp.maxValue + ls.size + 1
    val seq = ls.size + 1
    if (seq < maxValue) {
      sql"""insert into label_metas(label_id, name, seq, default_value, data_type, used_in_index) 
    select ${labelId}, ${name}, ${seq}, ${defaultValue}, ${dataType}, ${usedInIndex}"""
        .updateAndReturnGeneratedKey.apply()
    }
  }

  def findOrInsert(labelId: Int, name: String,
    defaultValue: String, dataType: String, usedInIndex: Boolean): LabelMeta = {
    //    play.api.Logger.debug(s"findOrInsert: $labelId, $name")
    findByName(labelId, name) match {
      case Some(c) => c
      case None =>
        insert(labelId, name, defaultValue, dataType, usedInIndex)
        val cacheKey = s"labelId=$labelId:name=$name"
        expireCache(cacheKey)
        findByName(labelId, name, false).get
    }
  }

  def delete(id: Int) = {
    val labelMeta = findById(id)
    val (labelId, name) = (labelMeta.labelId, labelMeta.name)
    sql"""delete from label_metas where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"labelId=$labelId", s"labelId=$labelId:name=$name")
    cacheKeys.foreach(expireCache(_))
  }

  def convert(labelId: Int, jsValue: JsValue): Map[Byte, InnerVal] = {
    val ret = for {
      (k, v) <- jsValue.as[JsObject].fields
      meta <- LabelMeta.findByName(labelId, k)
      innerVal <- jsValueToInnerVal(v, meta.dataType)
    } yield (meta.seq, innerVal)
    ret.toMap
  }
}

case class LabelMeta(id: Option[Int], labelId: Int, name: String, seq: Byte, defaultValue: String, dataType: String, usedInIndex: Boolean) extends JSONParser {
  lazy val defaultInnerVal = if (defaultValue.isEmpty) InnerVal.withStr("") else toInnerVal(defaultValue, dataType)
  lazy val toJson = Json.obj("name" -> name, "defaultValue" -> defaultValue, "dataType" -> dataType, "usedInIndex" -> usedInIndex)

}  