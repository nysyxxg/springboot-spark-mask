package com.bigdaf.sparkmask.hierarchy.impl

import com.bigdaf.sparkmask.hierarchy.Hierarchy
import com.bigdaf.sparkmask.hierarchy.mask.IdRule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class IdHierarchy(id:String,maskChar:Char)extends Hierarchy[String,String]{
  override def getIpRule(hierarchy: Int): String = {
      new IdRule(hierarchy,maskChar).mask(id)
}
}
