package com.bigdaf.sparkmask.hierarchy.function

import com.bigdaf.sparkmask.hierarchy.function.IdMaskFunctions.idMask
import com.bigdaf.sparkmask.hierarchy.impl.{EmailHierarchy, IdHierarchy}
import org.apache.spark.sql.functions.udf


/**
  * 自定义udf，对email列数据按要求进行脱敏处理
  * emailMask函数的第一个参数为列名，第二个参数为脱敏等级
  *
  */
object EmailMaskFunctions {
  val maskEmail = udf(emailMask(_: String, _: Int))

  def emailMask(email: String, level: Int): String = {
    val emailHierarchy = new EmailHierarchy(email, '*')
    emailHierarchy.getIpRule(level)
  }
}