package com.bigdaf.sparkmask.hierarchy.impl

import com.bigdaf.sparkmask.hierarchy.Hierarchy
import com.bigdaf.sparkmask.hierarchy.mask.RangeRule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class ageHierarchy[T:Numeric:ClassTag:TypeTag](bottom:T,top:T,step:T,level:Int,age:T)extends Hierarchy[T,Int]{



  /*def getMaskRule(hierarchy:Int):(T)=>Int={
     val list=new mutable.MutableList[T]
     var boundary=bottom
     val num=implicitly[Numeric[T]]
     var cuStep=step
     for(i<-1 until hierarchy){
         cuStep=num.times(cuStep,step)

     }
     while(num.lt(boundary,top)){
        list+=boundary
        boundary=num.plus(boundary,cuStep)
     }
     list+=top
     new RangeRule[T](list.toArray[T]).mask

  }*/

  override def getIpRule(hierarchy: Int):Int = {
    val list=new mutable.MutableList[T]
    var boundary=bottom
    val num=implicitly[Numeric[T]]
    var cuStep=step
    for(i<-1 until hierarchy){
      cuStep=num.times(cuStep,step)

    }
    while(num.lt(boundary,top)){
      list+=boundary
      boundary=num.plus(boundary,cuStep)
    }
    list+=top
    new RangeRule[T](list.toArray[T]).mask(age)
  }
}
