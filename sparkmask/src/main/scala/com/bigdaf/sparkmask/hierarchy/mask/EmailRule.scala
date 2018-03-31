package com.bigdaf.sparkmask.hierarchy.mask

import org.apache.commons.lang3.StringUtils

class EmailRule(hierarchy:Int,maskChar:Char)extends GeneralizerRule[String,String]{
  override def mask(input: String): String = {
    if (input.length <= hierarchy) {
      StringUtils.repeat(maskChar, input.length)
    } else {

      (hierarchy) match {
        case (0) =>input
        case (1) =>maskChar + "@" + input.substring(input.indexOf("@") + 1, input.length)
        case (2) => maskChar + "@" + maskChar + "." + input.substring(input.indexOf(".") + 1, input.length)
        case (3) => maskChar + "@" + maskChar + "." + maskChar
      }
    }
  }
}
