package com.kakao.ml.util

import java.lang.reflect.Method

case class PrivateMethodAccessor(instance: AnyRef, name: String) {

  val method: Method = {
    val methods = instance.getClass.getDeclaredMethods
    val method = methods.find(_.getName == name)
        .getOrElse(throw new IllegalArgumentException(
          s"no such method $name in ${instance.getClass}, available methods: ${methods.map(_.getName).mkString("\n")}"))
    method.setAccessible(true)
    method
  }

  def apply[T](args: Any*): T = method.invoke(instance, args.map(_.asInstanceOf[AnyRef]): _*).asInstanceOf[T]

}
