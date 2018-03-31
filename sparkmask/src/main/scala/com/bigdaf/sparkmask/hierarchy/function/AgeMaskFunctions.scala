package com.bigdaf.sparkmask.hierarchy.function

import com.bigdaf.sparkmask.hierarchy.function.IpMaskFunctions.ipMask
import com.bigdaf.sparkmask.hierarchy.impl.{IpHierarchy, ageHierarchy}
import org.apache.spark.sql.functions.udf

object AgeMaskFunctions {
  val maskAge = udf(ageMask(_: String, _: Int))

  def ageMask(age: String, level: Int): Int = {
    val ageHierarchy = new ageHierarchy[Int](10, 60, 10, 2, age.toInt)
    ageHierarchy.getIpRule(level)
  }
}