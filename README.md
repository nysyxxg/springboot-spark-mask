# springboot-spark-mask
1.本项目是需要解决的问题就是结合web前端和spark大数据引擎处理的结合，使用springboot作为web框架，实现前端和后台的交互。
2.spark来处理hive表数据里的数据，实现数据脱敏，然后输出保存。
3.前台用户只要输入需要脱敏的hive库名和表名，封装为json字符串，后台解析json为对象，然后传给sparksql。
4.sparksql加载数据.根据需求提取hive表中的各个敏感数据列，用udf自定义函数进行脱敏，最后把各个脱敏后的列和未脱敏的列重新连接起来。
5.把sparksql转为rdd，然后存入hbase中。
