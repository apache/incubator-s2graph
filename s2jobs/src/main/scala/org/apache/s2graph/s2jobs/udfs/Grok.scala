package org.apache.s2graph.s2jobs.udfs

import org.apache.s2graph.s2jobs.utils.GrokHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import play.api.libs.json.{JsValue, Json}

class Grok extends Udf {
  import org.apache.spark.sql.functions.udf

  def register(ss: SparkSession, name:String, options:Map[String, String]) = {
    // grok
    val patternDir = options.getOrElse("patternDir", "/tmp")
    val patternFiles = options.getOrElse("patternFiles", "").split(",").toSeq
    val patterns = Json.parse(options.getOrElse("patterns", "{}")).asOpt[Map[String, String]].getOrElse(Map.empty)
    val compilePattern = options("compilePattern")
    val schemaOpt = options.get("schema")

    patternFiles.foreach { patternFile =>
      ss.sparkContext.addFile(s"${patternDir}/${patternFile}")
    }

    implicit val grok = GrokHelper.getGrok(name, patternFiles, patterns, compilePattern)

    val f = if(schemaOpt.isDefined) {
      val schema = DataType.fromJson(schemaOpt.get)
      implicit val keys:Array[String] = schema.asInstanceOf[StructType].fieldNames
      udf(GrokHelper.grokMatchWithSchema _, schema)
    } else {
      udf(GrokHelper.grokMatch _)
    }


    ss.udf.register(name, f)
  }
}
