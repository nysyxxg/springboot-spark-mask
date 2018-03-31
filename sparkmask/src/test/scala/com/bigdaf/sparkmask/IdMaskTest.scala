package com.bigdaf.sparkmask

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions.lit
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}
import com.bigdaf.sparkmask.hierarchy.function._
import com.bigdaf.sparkmask.save.impl.WriteToHbase
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

class IdMaskTest {

  private val paymentCsvFile = "src/test/resources/payment.csv"
  private var sqlContext: SQLContext = _
  private val sc:SparkContext=_

  @Before
  def initiate: Unit = {
    sqlContext = new SQLContext(new SparkContext("local[2]", "Csvsuite"))
  }

  @After
  def close: Unit = {
    sqlContext.sparkContext.stop()
  }

  def paymentData: DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load(paymentCsvFile)
  }

  @Test
  def testIdMaskExpression: Unit = {
    /**
      * 测试id脱敏
      */
    val maskedIds = paymentData.select(IdMaskFunctions.maskId(new Column("id"),lit(2)))
      .collect().foreach(println(_))
  }
  @Test
  def testIpMaskExpression: Unit = {
    /**
      * 测试ip脱敏
      */
    val maskedIps = paymentData.select(IpMaskFunctions.maskIP(new Column("pos"),lit(2)))
      .collect().foreach(println(_))
  }
  @Test
  def testAgeMaskExpression: Unit = {
    /**
      * 测试age脱敏
      */
    val maskedAges = paymentData.select(AgeMaskFunctions.maskAge(new Column("age"),lit(2)))
      .collect().foreach(println(_))
  }
  @Test
  def testEmailMaskExpression: Unit = {
    /**
      * 测试age脱敏
      */
    val maskedEmail = paymentData.select(EmailMaskFunctions.maskEmail(new Column("age"),lit(1)))
      .collect().foreach(println(_))
  }

  /**
    * 对表里的所有敏感字段脱敏后输出表里的数据
    */
  @Test
  def testTableMaskExpression: Unit = {
    /**
      * 测试age脱敏
      */
   val maskRDD=paymentData.select(
             new Column("timestamp"),
             new Column("id0"),
             new Column("date"),
             EmailMaskFunctions.maskEmail(new Column("email"),lit(1)),
             IpMaskFunctions.maskIP(new Column("pos"),lit(2)),
             IdMaskFunctions.maskId(new Column("id"),lit(2)),
             new Column("mobile"),
             new Column("phone"),
             //EmailMaskFunctions.maskEmail(new Column("age"),lit(1)),
             new Column("age"),
             new Column("zipcode"),
             new Column("account"),
             new Column("cost"),
             new Column("location")).rdd
   // val writeHbase=new WriteToHbase
    maskRDD.map{m=>{
        /* Map("timestamp"->m(0),"id0"->m(1),"date"->m(2),"email"->m(3),"pos"->m(4),"id"->m(5),"mobile"->m(6),
           "phone"->m(7),"age"->m(8),"zipcode"->m(9),"account"->m(10),"cost"->m(11),"location"->m(12))*/
        /*一个Put对象就是一行记录，在构造方法中指定主键
        * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
        * Put.add方法接收三个参数：列族，列名，数据
        */
      sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "hadoop04,hadoop05,hadoop06")
      sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
      sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "tbs")
      //--创建conf对象
      val job = new Job(sc.hadoopConfiguration)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      val confx = job.getConfiguration()
        val put = new Put(Bytes.toBytes(m(0).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(m(1).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(m(2).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(m(3).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(m(4).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(m(5).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(m(6).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(m(7).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(m(8).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(m(9).toString))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(m(10).toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}.saveAsNewAPIHadoopDataset(confx)

  }
}
