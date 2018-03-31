package com.bigdaf.sparkmask.scalautil

import com.bigdaf.sparkmask.constants.Constants
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object InitUtils {


  /**
    * initialize spark/hiveContext
    */
  def initSparkContext():(SparkContext,HiveContext)={
         //initialize SparkConf
        val conf=getSparkConf()
        //initialize SparkContext
        val sc=new SparkContext(conf)
        //initialize HiveContext
        val hc=getSqlContext(sc)
        (sc,hc)

  }





  def getSparkConf():SparkConf={
      val local=Constants.SPARK_LOCAL_IF.toBoolean

     if(local)

         new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster(Constants.SPARK_MASTER)
     else
         new SparkConf().setAppName(Constants.SPARK_APP_NAME)

  }
  def getSqlContext(sc:SparkContext): HiveContext={
         val hc=new HiveContext(sc)
          hc

  }
}
