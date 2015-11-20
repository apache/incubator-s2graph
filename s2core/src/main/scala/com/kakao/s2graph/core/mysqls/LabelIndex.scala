package com.kakao.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */

import play.api.libs.json.Json
import scalikejdbc._

object LabelIndex extends Model[LabelIndex] {
  val DefaultName = "_PK"
  val DefaultMetaSeqs = Seq(LabelMeta.timeStampSeq)
  val DefaultSeq = 1.toByte
  val MaxOrderSeq = 7

  def apply(rs: WrappedResultSet): LabelIndex = {
    LabelIndex(rs.intOpt("id"), rs.int("label_id"), rs.string("name"), rs.byte("seq"),
      rs.string("meta_seqs").split(",").filter(_ != "").map(s => s.toByte).toList match {
        case metaSeqsList => metaSeqsList
      },
      rs.string("formulars"))
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession) = {
    val cacheKey = "id=" + id
    withCache(cacheKey) {
      sql"""select * from label_indices where id = ${id}""".map { rs => LabelIndex(rs) }.single.apply
    }.get
  }

  def findByLabelIdAll(labelId: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    val cacheKey = "labelId=" + labelId
    if (useCache) {
      withCaches(cacheKey)( sql"""
        select * from label_indices where label_id = ${labelId} and seq > 0 order by seq ASC
      """.map { rs => LabelIndex(rs) }.list.apply)
    } else {
      sql"""
        select * from label_indices where label_id = ${labelId} and seq > 0 order by seq ASC
      """.map { rs => LabelIndex(rs) }.list.apply
    }
  }

  def insert(labelId: Int, indexName: String, seq: Byte, metaSeqs: List[Byte], formulars: String)(implicit session: DBSession = AutoSession): Long = {
    sql"""
    	insert into label_indices(label_id, name, seq, meta_seqs, formulars)
    	values (${labelId}, ${indexName}, ${seq}, ${metaSeqs.mkString(",")}, ${formulars})
    """
      .updateAndReturnGeneratedKey.apply()
  }

  def findOrInsert(labelId: Int, indexName: String, metaSeqs: List[Byte], formulars: String)(implicit session: DBSession = AutoSession): LabelIndex = {
    findByLabelIdAndSeqs(labelId, metaSeqs) match {
      case Some(s) => s
      case None =>
        val orders = findByLabelIdAll(labelId, false)
        val seq = (orders.size + 1).toByte
        assert(seq <= MaxOrderSeq)
        val createdId = insert(labelId, indexName, seq, metaSeqs, formulars)
        val cacheKeys = List(s"labelId=$labelId:seq=$seq",
          s"labelId=$labelId:seqs=$metaSeqs", s"labelId=$labelId:seq=$seq", s"id=$createdId")
        cacheKeys.foreach { key =>
          expireCache(key)
          expireCaches(key)
        }

        findByLabelIdAndSeq(labelId, seq).get
    }
  }

  def findByLabelIdAndSeqs(labelId: Int, seqs: List[Byte])(implicit session: DBSession = AutoSession): Option[LabelIndex] = {
    val cacheKey = "labelId=" + labelId + ":seqs=" + seqs.mkString(",")
    withCache(cacheKey) {
      sql"""
      select * from label_indices where label_id = ${labelId} and meta_seqs = ${seqs.mkString(",")}
      """.map { rs => LabelIndex(rs) }.single.apply
    }
  }

  def findByLabelIdAndSeq(labelId: Int, seq: Byte, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[LabelIndex] = {
    //    val cacheKey = s"labelId=$labelId:seq=$seq"
    val cacheKey = "labelId=" + labelId + ":seq=" + seq
    if (useCache) {
      withCache(cacheKey)( sql"""
      select * from label_indices where label_id = ${labelId} and seq = ${seq}
      """.map { rs => LabelIndex(rs) }.single.apply)
    } else {
      sql"""
      select * from label_indices where label_id = ${labelId} and seq = ${seq}
      """.map { rs => LabelIndex(rs) }.single.apply
    }
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val labelIndex = findById(id)
    val seqs = labelIndex.metaSeqs.mkString(",")
    val (labelId, seq) = (labelIndex.labelId, labelIndex.seq)
    sql"""delete from label_indices where id = ${id}""".execute.apply()

    val cacheKeys = List(s"id=$id", s"labelId=$labelId", s"labelId=$labelId:seq=$seq", s"labelId=$labelId:seqs=$seqs")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
  }

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from label_indices""".map { rs => LabelIndex(rs) }.list.apply
    putsToCache(ls.map { x =>
      var cacheKey = s"id=${x.id.get}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      var cacheKey = s"labelId=${x.labelId}:seq=${x.seq}}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      var cacheKey = s"labelId=${x.labelId}:seqs=${x.metaSeqs.mkString(",")}"
      (cacheKey -> x)
    })
    putsToCaches(ls.groupBy(x => x.labelId).map { case (labelId, ls) =>
      val cacheKey = s"labelId=${labelId}"
      (cacheKey -> ls)
    }.toList)
  }
}

/**
 * formular
 * ex1): w1, w2, w3
 * ex2): 1.5 * w1^2 + 3.4 * (w1 * w2), w2, w1
 */

case class LabelIndex(id: Option[Int], labelId: Int, name: String, seq: Byte, metaSeqs: Seq[Byte], formulars: String) {
  lazy val label = Label.findById(labelId)
  lazy val metas = label.metaPropsMap
  lazy val sortKeyTypes = metaSeqs.flatMap(metaSeq => label.metaPropsMap.get(metaSeq))
  lazy val propNames = sortKeyTypes.map { labelMeta => labelMeta.name }
  lazy val toJson = Json.obj(
    "name" -> name,
    "propNames" -> sortKeyTypes.map(x => x.name)
  )
}
