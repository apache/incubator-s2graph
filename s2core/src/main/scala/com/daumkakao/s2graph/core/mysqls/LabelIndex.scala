package com.daumkakao.s2graph.core.mysqls

/**
 * Created by shon on 6/3/15.
 */
import scalikejdbc._
import play.api.libs.json.Json

object LabelIndex extends Model[LabelIndex] {

  val timestamp = LabelIndex(None, 0, 0.toByte, List(0), "")
  //  val withTsSeq = 0.toByte
  val defaultSeq = 1.toByte
  val maxOrderSeq = 7

  def apply(rs: WrappedResultSet): LabelIndex = {
    LabelIndex(rs.intOpt("id"), rs.int("label_id"), rs.byte("seq"),
      rs.string("meta_seqs").split(",").filter(_ != "").map(s => s.toByte).toList match {
        //        case metaSeqsList => if (metaSeqsList.isEmpty) List[Byte](timestamp.seq)  else metaSeqsList
        case metaSeqsList => metaSeqsList
      },
      rs.string("formulars"))
  }
  def findById(id: Int) = {
    val cacheKey = s"id=$id"
    withCache(cacheKey) {
      sql"""select * from label_indices where id = ${id}""".map { rs => LabelIndex(rs) }.single.apply
    }.get
  }
  def findByLabelIdAll(labelId: Int, useCache: Boolean = true) = {
    val cacheKey = s"labelId=$labelId"
    if (useCache) {
      withCaches(cacheKey)(sql"""
        select * from label_indices where label_id = ${labelId} and seq > 0
      """.map { rs => LabelIndex(rs) }.list.apply)
    } else {
      sql"""
        select * from label_indices where label_id = ${labelId} and seq > 0
      """.map { rs => LabelIndex(rs) }.list.apply
    }
  }
  def insert(labelId: Int, seq: Byte, metaSeqs: List[Byte], formulars: String): Long = {
    sql"""
    	insert into label_indices(label_id, seq, meta_seqs, formulars)
    	values (${labelId}, ${seq}, ${metaSeqs.mkString(",")}, ${formulars})
    """
      .updateAndReturnGeneratedKey.apply()
  }

  def findOrInsert(labelId: Int, seq: Byte, metaSeqs: List[Byte], formulars: String): LabelIndex = {
    //    kgraph.Logger.debug(s"findOrInsert: $labelId, $seq, $metaSeqs, $formulars")
    findByLabelIdAndSeq(labelId, seq) match {
      case Some(s) => s
      case None =>
        val createdId = insert(labelId, seq, metaSeqs, formulars)
        val cacheKeys = List(s"labelId=$labelId:seq=$seq",
        s"labelId=$labelId:seqs=$metaSeqs", s"labelId=$labelId:seq=$seq", s"id=$createdId")
        cacheKeys.foreach(expireCache(_))
        findByLabelIdAndSeq(labelId, seq, false).get // forcely find label information form label_indices table, add false(don't use cache) option
    }
  }
  def findOrInsert(labelId: Int, metaSeqs: List[Byte], formulars: String): LabelIndex = {
    //    Logger.debug(s"findOrInsert: $labelId, $seq, $metaSeqs, $formulars")
    findByLabelIdAndSeqs(labelId, metaSeqs) match {
      case Some(s) => s
      case None =>
        val orders = findByLabelIdAll(labelId, false)
        val seq = (orders.size + 1).toByte
        assert(seq <= maxOrderSeq)
        val createdId = insert(labelId, seq, metaSeqs, formulars)
        val cacheKeys = List(s"labelId=$labelId:seq=$seq",
          s"labelId=$labelId:seqs=$metaSeqs", s"labelId=$labelId:seq=$seq", s"id=$createdId")
        cacheKeys.foreach(expireCache(_))
        findByLabelIdAndSeq(labelId, seq).get
    }
  }

  def findByLabelIdAndSeqs(labelId: Int, seqs: List[Byte]): Option[LabelIndex] = {
    //    val seqsStr = seqs.sortBy(x => x).mkString(",")
    val cacheKey = s"labelId=$labelId:seqs=${seqs.mkString(",")}"
    withCache(cacheKey) {
      sql"""
      select * from label_indices where label_id = ${labelId} and meta_seqs = ${seqs.mkString(",")}
      """.map { rs => LabelIndex(rs) }.single.apply
    }
  }
  def findByLabelIdAndSeq(labelId: Int, seq: Byte, useCache: Boolean = true): Option[LabelIndex] = {
    val cacheKey = s"labelId=$labelId:seq=$seq"
    val fetch = sql"""
      select * from label_indices where label_id = ${labelId} and seq = ${seq}
      """.map { rs => LabelIndex(rs) }.single.apply
    if (useCache) {
      withCache(cacheKey)(fetch)
    } else {
      fetch
    }
  }
  def delete(id: Int) = {
    val labelIndex = findById(id)
    val seqs = labelIndex.metaSeqs.mkString(",")
    val (labelId, seq) = (labelIndex.labelId, labelIndex.seq)
    sql"""delete from label_indices where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"labelId=$labelId", s"labelId=$labelId:seq=$seq", s"labelId=$labelId:seqs=$seqs")
    cacheKeys.foreach(expireCache(_))
  }
}

/**
 * formular
 * ex1): w1, w2, w3
 * ex2): 1.5 * w1^2 + 3.4 * (w1 * w2), w2, w1
 */

case class LabelIndex(id: Option[Int], labelId: Int, seq: Byte, metaSeqs: List[Byte], formulars: String) {

  lazy val label = Label.findById(labelId)
  lazy val metas = label.metaPropsMap
  lazy val sortKeyTypes = metaSeqs.map(metaSeq => label.metaPropsMap.get(metaSeq)).flatten
//  lazy val sortKeyTypeDefaultVals = sortKeyTypes.map(x => x.defaultInnerVal)
  lazy val toJson = Json.obj("indexProps" -> sortKeyTypes.map(x => x.name))
}
