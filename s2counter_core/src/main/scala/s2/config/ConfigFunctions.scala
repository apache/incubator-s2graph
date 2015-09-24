package s2.config

import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 3. 2..
 */
abstract class ConfigFunctions(conf: Config) {
  def getOrElse[T: ClassTag](key: String, default: T): T = {
    val ret = if (conf.hasPath(key)) (default match {
      case _: String => conf.getString(key)
      case _: Int | _: Integer => conf.getInt(key)
      case _: Float | _: Double => conf.getDouble(key)
      case _: Boolean => conf.getBoolean(key)
      case _ => default
    }).asInstanceOf[T]
    else default
    println(s"${this.getClass.getName}: $key -> $ret")
    ret
  }

  def getConfigMap(path: String): Map[String, String] = {
    conf.getConfig(path).entrySet().map { entry =>
      val key = s"$path.${entry.getKey}"
      val value = conf.getString(key)
      println(s"${this.getClass.getName}: $key -> $value")
      key -> value
    }.toMap
  }
}
