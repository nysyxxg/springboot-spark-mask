package com.bigdaf.sparkmask.hierarchy.mask

import org.apache.commons.lang3.StringUtils

class TimestampRule(hierarchy:Int,maskChar:Char) extends GeneralizerRule[String,String]{
  override def mask(input: String): String ={
       if(input.length<=hierarchy){
           StringUtils.repeat(maskChar,input.length)
       }else{
         (hierarchy) match{
           case (0)=>input
           case (1)=>input.substring(0,input.length-4)
           case (2) =>
             input.substring(0, input.length - 6) + "00"
           case (3) =>
             input.substring(0, input.length - 9) + "00:00"
           case (4) =>
             input.substring(0, input.length - 12) + "00:00:00"
           case (5) =>
             input.substring(0, input.length - 15) + "01T00:00:00"
           case (6) =>
             input.substring(0, input.length - 18) + "01-01T00:00:00"
           case (7) =>
             "1901-01-01T00:00:00"
         }
       }
  }
}
