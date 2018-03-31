package com.bigdaf.sparkmask.hierarchy.mask

class NumericRanges[T:Numeric](boundaries:Array[T]) extends Serializable {
     def getRangeIndex(input:T):Int={
           binarySearch(boundaries,0,boundaries.length,input)
     }
     private def binarySearch(boundaries:Array[T],start:Int,end:Int,input:T):Int={
       val checkpoint=(end+start)/2
       val compFloor:Int=implicitly[Numeric[T]].compare(boundaries(checkpoint),input)
       val compCeil:Int=implicitly[Numeric[T]].compare(boundaries(checkpoint+1),input)
       if(compFloor==0){
          return checkpoint
       }
       if(compCeil==0){
         return checkpoint+1
       }
       (compFloor>0,compCeil>0) match {
         case (true,true)=>binarySearch(boundaries,start,checkpoint,input)
         case (false,false)=>binarySearch(boundaries,checkpoint,end,input)
         case (false,true)=>checkpoint
         case _=>throw new RuntimeException("bad boundaries:" + boundaries.mkString(","))
       }
  }
}
