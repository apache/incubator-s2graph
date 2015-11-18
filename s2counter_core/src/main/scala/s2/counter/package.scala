package s2

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 22..
 */
package object counter {
  val VERSION_1: Byte = 1
  val VERSION_2: Byte = 2

  case class MethodNotSupportedException(message: String, cause: Throwable = null) extends Exception(message, cause)
}
