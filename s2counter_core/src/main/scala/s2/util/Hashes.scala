package s2.util

import org.apache.hadoop.hbase.util.Bytes

import scala.util.hashing.MurmurHash3

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 27..
 */
object Hashes {
  def sha1(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    Bytes.toHex(md.digest(s.getBytes("UTF-8")))
  }
  
  private def positiveHash(h: Int): Int = {
    if (h < 0) -1 * (h + 1) else h
  }

  def murmur3(s: String): Int = {
    val hash = MurmurHash3.stringHash(s)
    positiveHash(hash)
  }
}
