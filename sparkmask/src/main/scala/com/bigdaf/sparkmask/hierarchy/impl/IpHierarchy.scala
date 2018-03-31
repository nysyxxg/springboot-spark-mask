package com.bigdaf.sparkmask.hierarchy.impl

import com.bigdaf.sparkmask.hierarchy.Hierarchy
import com.bigdaf.sparkmask.hierarchy.mask.IpRule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class IpHierarchy(alignLeft: Boolean, maskLeft: Boolean, ip:String,maskChar:Char)extends Hierarchy[String,String]{

  override def getIpRule(hierarchy:Int):String={
      new IpRule(alignLeft,maskLeft,hierarchy,maskChar).mask(ip)
  }
}
