package com.kakao.s2graph.core

import scala.util.hashing.MurmurHash3
import java.util.regex.Pattern
import play.api.libs.json.Json

object GraphUtil {
  private val TOKEN_DELIMITER = Pattern.compile("[\t]")
  val operations = Map("i" -> 0, "insert" -> 0, "u" -> 1, "update" -> 1,
    "increment" -> 2, "d" -> 3, "delete" -> 3,
    "deleteAll" -> 4, "insertBulk" -> 5, "incrementCount" -> 6).map {
    case (k, v) =>
      k -> v.toByte
  }
  val BitsForMurMurHash = 16
  val bytesForMurMurHash = 2
  val defaultOpByte = operations("insert")
  val directions = Map("out" -> 0, "in" -> 1, "undirected" -> 2, "u" -> 2, "directed" -> 0, "d" -> 0)
  val consistencyLevel = Map("weak" -> 0, "strong" -> 1)

  def toType(t: String) = {
    t.trim().toLowerCase match {
      case "e" | "edge" => "edge"
      case "v" | "vertex" => "vertex"
    }
  }

  def toDir(direction: String): Option[Byte] = {
    val d = direction.trim().toLowerCase match {
      case "directed" | "d" => Some(0)
      case "undirected" | "u" => Some(2)
      case "out" => Some(0)
      case "in" => Some(1)
      case _ => None
    }
    d.map(x => x.toByte)
  }

  def toDirection(direction: String): Int = {
    direction.trim().toLowerCase match {
      case "directed" | "d" => 0
      case "undirected" | "u" => 2
      case "out" => 0
      case "in" => 1
      case _ => 2
    }
  }

  def fromDirection(direction: Int) = {
    direction match {
      case 0 => "out"
      case 1 => "in"
      case 2 => "undirected"
    }
  }

  def toggleDir(dir: Int) = {
    dir match {
      case 0 => 1
      case 1 => 0
      case 2 => 2
      case _ => throw new UnsupportedOperationException(s"toggleDirection: $dir")
    }
  }

  def toOp(op: String): Option[Byte] = {
    op.trim() match {
      case "i" | "insert" => Some(0)
      case "d" | "delete" => Some(3)
      case "u" | "update" => Some(1)
      case "increment" => Some(2)
      case "deleteAll" => Some(4)
      case "insertBulk" => Some(5)
      case "incrementCount" => Option(6)
      case _ => None
    }
  }

  def fromOp(op: Byte): String = {
    op match {
      case 0 => "insert"
      case 3 => "delete"
      case 1 => "update"
      case 2 => "increment"
      case 4 => "deleteAll"
      case 5 => "insertBulk"
      case 6 => "incrementCount"
      case _ =>
        throw new UnsupportedOperationException(s"op : $op (only support 0(insert),1(delete),2(updaet),3(increment))")
    }
  }

  //  def toggleOp(op: Byte): Byte = {
  //    val ret = op match {
  //      case 0 => 1
  //      case 1 => 0
  //      case x: Byte => x
  //    }
  //    ret.toByte
  //  }
  // 2^31 - 1
  def transformHash(h: Int): Int = {
    //    h / 2 + (Int.MaxValue / 2 - 1)
    if (h < 0) -1 * (h + 1) else h
  }

  def murmur3(s: String): Short = {
    val hash = MurmurHash3.stringHash(s)
    val positiveHash = transformHash(hash) >> BitsForMurMurHash
    positiveHash.toShort
  }

  def smartSplit(s: String, delemiter: String) = {
    val trimed_string = s.trim()
    if (trimed_string.equals("")) {
      Seq[String]()
    } else {
      trimed_string.split(delemiter).toList
    }
  }

  def split(line: String) = TOKEN_DELIMITER.split(line)

  def parseString(s: String): List[String] = {
    if (!s.startsWith("[")) {
      s.split("\n").toList
    } else {
      Json.parse(s).asOpt[List[String]].getOrElse(List.empty[String])
    }
  }

}
