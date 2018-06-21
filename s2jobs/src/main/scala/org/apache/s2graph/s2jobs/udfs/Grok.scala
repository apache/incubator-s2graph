package org.apache.s2graph.s2jobs.udfs

import org.apache.s2graph.s2jobs.utils.GrokHelper
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

class Grok extends Udf {
  def register(ss: SparkSession, name:String, options:Map[String, String]) = {
    // grok
    val patternDir = options.getOrElse("patternDir", "/tmp")
    val patternFiles = options.getOrElse("patternFiles", "").split(",").toSeq
    val patterns = Json.parse(options.getOrElse("patterns", "{}")).asOpt[Map[String, String]].getOrElse(Map.empty)
    val compilePattern = options("compilePattern")

    patternFiles.foreach { patternFile =>
      ss.sparkContext.addFile(s"${patternDir}/${patternFile}")
    }
    implicit val grok = GrokHelper.getGrok(name, patternFiles, patterns, compilePattern)
    ss.udf.register(name, GrokHelper.grokMatch _)
  }
}
