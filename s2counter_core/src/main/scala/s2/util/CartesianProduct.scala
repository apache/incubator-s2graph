package s2.util

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 19..
 */
object CartesianProduct {
  def apply[T](xss: List[List[T]]): List[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t => for(xh <- h; xt <- apply(t)) yield xh :: xt
  }
}
