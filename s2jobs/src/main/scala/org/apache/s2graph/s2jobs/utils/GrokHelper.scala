package org.apache.s2graph.s2jobs.utils

import io.thekraken.grok.api.Grok
import org.apache.s2graph.s2jobs.Logger
import org.apache.spark.SparkFiles
import org.apache.spark.sql.Row

import scala.collection.mutable

object GrokHelper extends Logger {
  private val grokPool:mutable.Map[String, Grok] = mutable.Map.empty

  def getGrok(name:String, patternFiles:Seq[String], patterns:Map[String, String], compilePattern:String):Grok = {
    if (grokPool.get(name).isEmpty) {
      println(s"Grok '$name' initialized..")
      val grok = new Grok()
      patternFiles.foreach { patternFile =>
        val filePath = SparkFiles.get(patternFile)
        println(s"[Grok][$name] add pattern file : $patternFile  ($filePath)")
        grok.addPatternFromFile(filePath)
      }
      patterns.foreach { case (name, pattern) =>
        println(s"[Grok][$name] add pattern : $name ($pattern)")
        grok.addPattern(name, pattern)
      }

      grok.compile(compilePattern)
      println(s"[Grok][$name] patterns: ${grok.getPatterns}")
      grokPool.put(name, grok)
    }

    grokPool(name)
  }

  def grokMatch(text:String)(implicit grok:Grok):Option[Map[String, String]] = {
    import scala.collection.JavaConverters._

    val m = grok.`match`(text)
    m.captures()
    val rstMap = m.toMap.asScala.toMap
      .filter(_._2 != null)
      .map{ case (k, v) =>  k -> v.toString}
    if (rstMap.isEmpty) None else Some(rstMap)
  }

  def grokMatchWithSchema(text:String)(implicit grok:Grok, keys:Array[String]):Option[Row] = {
    import scala.collection.JavaConverters._

    val m = grok.`match`(text)
    m.captures()

    val rstMap = m.toMap.asScala.toMap
    if (rstMap.isEmpty) None
    else {
      val l = keys.map { key => rstMap.getOrElse(key, null)}
      Some(Row.fromSeq(l))
    }
  }
}
