package com.bigdaf.sparkmask.save.impl

import com.bigdaf.sparkmask.save.WriteData
import org.apache.spark.rdd.RDD

class WriteToHbase extends WriteData[String]{
  override def writeData(dataRDD: Array[String]): Unit ={

  }
}
