package com.bigdaf.sparkmask.constants;

public interface Constants {
    String SPARK_MASTER="local[2]";
    String SPARK_LOCAL="spark.local";
    String SPARK_APP_NAME="RuleBasedMaskForSparks";
    String SPARK_LOCAL_IF="true";
    String HIVE_TABLENAME="whp.payment";
    String HBASE_ZOOKEEPER="hbase.zookeeper.quorum";
    String ZOOKEEPER_COLONY="hadoop01,hadoop02,hadoop03";
    String ZOOKEEPER_PORT="hbase.zookeeper.property.clientPort";
    String HBASE_TABLE="userInfo";


}
