package com.bigdaf.sparkmask.save

import org.apache.spark.rdd.RDD

trait WriteData[T]{
    def writeData(dataRDD:Array[T])
}
