package com.erongda.bigdata.utils

/**
  * 存储一些常量数据
  */
object ConstantUtils {


  val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/"
  /**
    * 定义KAFKA相关集群配置信息
    */
  // 本地读取文件的目录路径
  val LOCAL_DATA_DIC = "file:///E:/IDEAworkspace/scalaLearning/src/main/data"

  // KAFKA CLUSTER BROKERS
  val METADATA_BROKER_LIST = "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094"

  // metadata.broker.list 的名称
  val METADATA_BROKER_LIST_NAME = "metadata.broker.list"

  // OFFSET
  val AUTO_OFFSET_RESET = "largest"

  // 序列化类
  val SERIALIZER_CLASS = "kafka.serializer.StringEncoder"
  val NEW_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"

  // 发送数据方式
  val PRODUCER_TYPE = "async"

  // Topic的名称
  val ORDER_TOPIC = "orders"

  // KAFKA 存储数据编码设置
  val SERIALIZER_CLASS_NAME = "serializer.class"
  val KEY_SERIALIZER_CLASS_NAME = "key.serializer.class"

  // 设置Streaming Application检查点目录
  val CHECK_POINT_DIRECTORY = "/datas/spark/streaming/ckptkk-00052" + System.currentTimeMillis()

}
