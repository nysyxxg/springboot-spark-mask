package com.bigdaf.sparkmask.hierarchy.function

import com.bigdaf.sparkmask.hierarchy.impl.IpHierarchy
import org.apache.spark.sql.functions.udf

object IpMaskFunctions {

  val maskIP = udf(ipMask(_: String, _: Int))

  def ipMask(ip: String, level: Int): String = {
    val ipHierarchy=new IpHierarchy(true,true,ip,'*')
       ipHierarchy.getIpRule(level)
  }

}