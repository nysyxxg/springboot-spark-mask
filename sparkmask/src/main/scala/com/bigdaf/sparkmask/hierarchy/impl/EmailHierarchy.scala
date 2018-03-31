package com.bigdaf.sparkmask.hierarchy.impl

import com.bigdaf.sparkmask.hierarchy.Hierarchy
import com.bigdaf.sparkmask.hierarchy.mask.EmailRule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class EmailHierarchy(email:String,maskChar:Char)extends Hierarchy[String,String]{
  override def getIpRule(hierarchy: Int): String = {
      new EmailRule(hierarchy,maskChar).mask(email)
}
}
