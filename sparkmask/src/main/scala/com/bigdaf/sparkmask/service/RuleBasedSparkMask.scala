package com.bigdaf.sparkmask.service



import com.bigdaf.sparkmask.constants.Constants
import com.bigdaf.sparkmask.hierarchy.function.{EmailMaskFunctions, IdMaskFunctions, IpMaskFunctions}
import com.bigdaf.sparkmask.hierarchy.impl.IpHierarchy
import com.bigdaf.sparkmask.scalautil.InitUtils
import com.bigdaf.sparkmask.vo.MaskVo
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.springframework.stereotype.Service
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes

@Service
class RuleBasedSparkMask{
   def MaskTask(maskVo:MaskVo): String = {
    val dbName=maskVo.getDbName.toString
    val table=maskVo.getTables.toString
     // println(tableName)
    //load sparkContstxt

    val context = InitUtils.initSparkContext()
    val sc = context._1
    val hc = context._2
    hc.table(dbName+"."+table).createOrReplaceTempView("payment")
    val hiveDF=hc.sql("select * from payment")
     val maskRDD=hiveDF.select(
       new Column("timestamp"),
       new Column("id0"),
       new Column("date"),
       EmailMaskFunctions.maskEmail(new Column("email"),lit(1)),
       IpMaskFunctions.maskIP(new Column("pos"),lit(2)),
       IdMaskFunctions.maskId(new Column("id"),lit(2)),
       new Column("mobile"),
       new Column("phone"),
       EmailMaskFunctions.maskEmail(new Column("age"),lit(1)),
       new Column("zipcode"),
       new Column("account"),
       new Column("cost"),
       new Column("location")).rdd
     val conf = HBaseConfiguration.create()
     //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
     conf.set(Constants.HBASE_ZOOKEEPER,Constants.ZOOKEEPER_COLONY)
     //设置zookeeper连接端口，默认2181
     conf.set(Constants.ZOOKEEPER_PORT, "2181")
     //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
     val jobConf = new JobConf(conf)
     jobConf.setOutputFormat(classOf[TableOutputFormat])
     jobConf.set(TableOutputFormat.OUTPUT_TABLE, Constants.HBASE_TABLE)
     val hiveRDD=maskRDD.map { m => {
       val put = new Put(Bytes.toBytes(m(0).toString+"_"+m(12).toString)+"_"+m(2).toString)
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("timestamp"), Bytes.toBytes(m(0).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("id0"), Bytes.toBytes(m(1).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(m(2).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("email"), Bytes.toBytes(m(3).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("pos"), Bytes.toBytes(m(4).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(m(5).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("mobile"), Bytes.toBytes(m(6).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("phone"), Bytes.toBytes(m(7).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(m(8).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("zipcode"), Bytes.toBytes(m(9).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("account"), Bytes.toBytes(m(10).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("cost"), Bytes.toBytes(m(11).toString))
       put.add(Bytes.toBytes("cf"), Bytes.toBytes("location"), Bytes.toBytes(m(12).toString))
       //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
       (new ImmutableBytesWritable, put)
     }}
     hiveRDD.saveAsHadoopDataset(jobConf)
     hc.sparkContext.stop()
     var isComplete=false
     if(hiveRDD.count()!=0){
       isComplete=true
     }
     if(isComplete){
         return "保存成功"
     }else{
         return "保存失败"
     }
  }
}