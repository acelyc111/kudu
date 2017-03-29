package org.apache.kudu.spark.kudu

object KuduUtil {
  def invokeMethod(obj: Any, methodName: String, args: Any*): Unit = {
    val classSeq = args.map(arg => arg.getClass)
    val method = obj.getClass.getDeclaredMethod(methodName, classSeq: _*)
    method.setAccessible(true)
    method.invoke(obj, args)
  }
}
