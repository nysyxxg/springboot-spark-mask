package com.bigdaf.sparkmask.hierarchy.mask

import org.apache.commons.lang3.StringUtils

class DateRule(hierarchy:Int,maskChar:Char)extends GeneralizerRule[String,String]{
  override def mask(input: String): String = {
      if(input.length<=hierarchy){
        StringUtils.repeat(maskChar,input.length)
      }else{
        (hierarchy) match {
          case (0)=>input
          case (1)=>input.substring(0,input.lastIndexOf("-")+1)+"01"
          case (2)=>input.substring(0,input.indexOf("-")+1)+"01-01"
          case (3)=>"1901-0101"
        }
      }
  }
}
