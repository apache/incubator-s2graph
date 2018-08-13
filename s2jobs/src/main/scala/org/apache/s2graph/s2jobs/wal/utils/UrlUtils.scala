package org.apache.s2graph.s2jobs.wal.utils

import java.net.{URI, URLDecoder}

import scala.util.matching.Regex

object UrlUtils {
  val pattern = new Regex("""(\\x[0-9A-Fa-f]{2}){3}""")
  val koreanPattern = new scala.util.matching.Regex("([가-힣]+[\\-_a-zA-Z 0-9]*)+|([\\-_a-zA-Z 0-9]+[가-힣]+)")


  // url extraction functions
  def urlDecode(url: String): (Boolean, String) = {
    try {
      val decoded = URLDecoder.decode(url, "UTF-8")
      (url != decoded, decoded)
    } catch {
      case e: Exception => (false, url)
    }
  }

  def hex2String(url: String): String = {
    pattern replaceAllIn(url, m => {
      new String(m.toString.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte), "utf-8")
    })
  }

  def toDomains(url: String, maxDepth: Int = 3): Seq[String] = {
    val uri = new URI(url)
    val domain = uri.getHost
    if (domain == null) Nil
    else {
      val paths = uri.getPath.split("/")
      if (paths.isEmpty) Seq(domain)
      else {
        val depth = Math.min(maxDepth, paths.size)
        (1 to depth).map { ith =>
          domain + paths.take(ith).mkString("/")
        }
      }
    }
  }

  def extract(_url: String): (String, Seq[String], Option[String]) = {
    try {
      val url = hex2String(_url)
      val (encoded, decodedUrl) = urlDecode(url)

      val kwdOpt = koreanPattern.findAllMatchIn(decodedUrl).toList.map(_.group(0)).headOption.map(_.replaceAll("\\s", ""))
      val domains = toDomains(url.replaceAll(" ", ""))
      (decodedUrl, domains, kwdOpt)
    } catch {
      case e: Exception => (_url, Nil, None)
    }
  }
}

