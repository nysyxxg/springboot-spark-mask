package com.bigdaf.sparkmask.hierarchy.mask

import scala.reflect.ClassTag

class RangeRule[T:Numeric:ClassTag](bounds:Array[T])extends GeneralizerRule[T,Int]{
  private val range=new NumericRanges[T](bounds)
  override def mask(input: T): Int = {
       range.getRangeIndex(input)
  }
}
