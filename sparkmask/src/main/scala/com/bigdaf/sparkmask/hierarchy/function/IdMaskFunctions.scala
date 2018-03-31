package com.bigdaf.sparkmask.hierarchy.function


import com.bigdaf.sparkmask.hierarchy.impl.{IdHierarchy}
import org.apache.spark.sql.functions.udf

object IdMaskFunctions {

  val maskId = udf(idMask(_: String, _: Int))

  def idMask(id: String, level: Int): String = {
    val idHierarchy = new IdHierarchy(id, '*')
    idHierarchy.getIpRule(level)
  }
}