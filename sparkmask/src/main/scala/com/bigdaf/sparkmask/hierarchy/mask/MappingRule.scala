package com.bigdaf.sparkmask.hierarchy.mask

class MappingRule[T](val hierarchyData:Array[(T,T)])extends GeneralizerRule[T,T]{
     override def mask(input: T): T = {
           val filtered:Array[(T,T)]=hierarchyData.filter(_._1==input)
            filtered(0)._2
     }
}
