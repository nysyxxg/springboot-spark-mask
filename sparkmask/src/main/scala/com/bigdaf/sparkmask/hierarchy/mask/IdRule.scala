package com.bigdaf.sparkmask.hierarchy.mask

import org.apache.commons.lang3.StringUtils

class IdRule(hierarchy:Int,maskChar:Char)extends GeneralizerRule[String,String]{
  override def mask(input: String): String ={
        if(input.length<=hierarchy){
          StringUtils.repeat(maskChar,input.length)
        }else{
          (hierarchy) match {
            case (0)=> input
            case (1)=>input.substring(0,input.length-1)+"X"
            case (2)=>input.substring(0,input.length-1)+"X"
            case (3)=>input.substring(0,input.length-1)+"a"
          }
        }
  }
}
