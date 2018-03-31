package com.bigdaf.sparkmask.hierarchy.mask

import org.apache.commons.lang3.StringUtils

class IpRule(alignLeft: Boolean, maskLeft: Boolean, hierarchy: Int, maskChar: Char)extends GeneralizerRule[String, String]  {
  override def mask(input: String): String = {
    (alignLeft, maskLeft) match {
      case (true, true) =>
        ""
      case (true, false) =>
        ""
      case (false, true) =>
        ""
      case (false, false) =>
        ""
    }

    if (input.length <= hierarchy) {
      StringUtils.repeat(maskChar, input.length)
    } else {
      input.substring(0, input.length - hierarchy) + StringUtils.repeat(maskChar, hierarchy)
    }
  }
}
