package com.kakao.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */


import com.kakao.s2graph.core.JSONParser
import com.kakao.s2graph.core.GraphExceptions.MaxPropSizeReachedException
import play.api.libs.json.Json
import scalikejdbc._

object LabelMeta extends Model[LabelMeta] with JSONParser {

  /** dummy sequences */

  val fromSeq = -4.toByte
  val toSeq = -5.toByte
  val lastOpSeq = -3.toByte
  val lastDeletedAt = -2.toByte
  val timeStampSeq = 0.toByte
  val countSeq = (Byte.MaxValue - 2).toByte
  val degreeSeq = (Byte.MaxValue - 1).toByte
  val maxValue = Byte.MaxValue
  val emptyValue = Byte.MaxValue

  /** reserved sequences */
  //  val deleted = LabelMeta(id = Some(lastDeletedAt), labelId = lastDeletedAt, name = "lastDeletedAt",
  //    seq = lastDeletedAt, defaultValue = "", dataType = "long")
  val from = LabelMeta(id = Some(fromSeq), labelId = fromSeq, name = "_from",
    seq = fromSeq, defaultValue = fromSeq.toString, dataType = "long")
  val to = LabelMeta(id = Some(toSeq), labelId = toSeq, name = "_to",
    seq = toSeq, defaultValue = toSeq.toString, dataType = "long")
  val timestamp = LabelMeta(id = Some(-1), labelId = -1, name = "_timestamp",
    seq = timeStampSeq, defaultValue = "0", dataType = "long")
  val degree = LabelMeta(id = Some(-1), labelId = -1, name = "_degree",
    seq = degreeSeq, defaultValue = "0", dataType = "long")
  val count = LabelMeta(id = Some(-1), labelId = -1, name = "_count",
    seq = countSeq, defaultValue = "-1", dataType = "long")
  val reservedMetas = List(from, to, degree, timestamp, count).flatMap { lm => List(lm, lm.copy(name = lm.name.drop(1))) }
  val notExistSeqInDB = List(lastOpSeq, lastDeletedAt, countSeq, degree, timeStampSeq, from.seq, to.seq)

  def apply(rs: WrappedResultSet): LabelMeta = {
    LabelMeta(Some(rs.int("id")), rs.int("label_id"), rs.string("name"), rs.byte("seq"), rs.string("default_value"), rs.string("data_type").toLowerCase)
  }

  def isValidSeq(seq: Byte): Boolean = seq >= 0 && seq <= countSeq
  def isValidSeqForAdmin(seq: Byte): Boolean = seq > 0 && seq < countSeq

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

  def insert(labelId: Int, name: String, defaultValue: String, dataType: String)(implicit session: DBSession = AutoSession) = {
    val ls = findAllByLabelId(labelId, useCache = false)
    val seq = ls.size + 1

    if (seq < maxValue) {
      sql"""insert into label_metas(label_id, name, seq, default_value, data_type)
    select ${labelId}, ${name}, ${seq}, ${defaultValue}, ${dataType}""".updateAndReturnGeneratedKey.apply()
    } else {
      throw MaxPropSizeReachedException("max property size reached")
    }
  }

  def findOrInsert(labelId: Int,
                   name: String,
                   defaultValue: String,
                   dataType: String)(implicit session: DBSession = AutoSession): LabelMeta = {

    findByName(labelId, name) match {
      case Some(c) => c
      case None =>
        insert(labelId, name, defaultValue, dataType)
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
    cacheKeys.foreach(expireCache)
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
  }
}

case class LabelMeta(id: Option[Int], labelId: Int, name: String, seq: Byte, defaultValue: String, dataType: String) extends JSONParser {
  lazy val toJson = Json.obj("name" -> name, "defaultValue" -> defaultValue, "dataType" -> dataType)
}
