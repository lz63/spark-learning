package com.erongda.bigdata.risk

import java.util.Properties

import com.erongda.bigdata.risk.hbase.HBaseDao
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * SparkStreaming 消费Kafka Topic中数据，存入到HBase表中
  *
  *   TODO： 相关说明
  *     -a. Kafka 中一个Topic中的数据，存储在一个HBase表中
  *       topic的名称与table的名称一致
  *     -b. 实际项目中使用Python依据用户的信息爬取不同维度的数据，存储在不同的Kafka topic中
  *       所有topic中的每条数据，都是JSON格式的数据，符合标准
  *       {
  *         'r': '00000',
  *         'f': 'info',
  *         'q': ['xx', 'xx', ....],
  *         'v': ['xx1', 'xx2', ...],
  *         't': 'timestamp
  *       }
  */
object KafkaTopicStreaming {

  // 记录日志信息
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    // 解释参数到变量
    // val Array(kafkaTopicIndex, batchInterval, maxRatePrePartition) = args
    val Array(kafkaTopicIndex, batchInterval, maxRatePrePartition) = Array("1", "3", "2000")

    /**
      * TODO: 1 -> 构建StreamingContext流式上下文对象
      */
    val (sc, ssc) = {
      // 1.1 初始化配置，SparkConf实例对象，应该相关配置
      val sparkConf = new SparkConf()
        .setAppName(KafkaDataStreamRedis.getClass.getSimpleName)
        // 设置运行的地方，如果是本地模式需要设置, 集群模式通过spark-submit提交传递
        .setMaster("local[3]")
        // 设置最大条目数
        .set("spark.streaming.kafka.maxRatePerPartition", maxRatePrePartition)
        .set("spark.streaming.backpressure.enabled", "true")
        // 设置序列化
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      // 1.2 创建SparkContext实例对象
      val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)
      sparkContext.setLogLevel("WARN")

      // 1.3 创建StreamingContext上下文对象，设置批处理时间间隔
      val streamingContext = new StreamingContext(sparkContext, Seconds(batchInterval.toInt))

      // 返回值
      (sparkContext, streamingContext)
    }

    // TODO: i. 加载配置文件
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("kafka.properties"))

    // TODO: ii. 从配置文件中获取Topic名称
    val kafkaTopics = props.getProperty(s"kafka.topic.$kafkaTopicIndex")
    if(kafkaTopics == null || kafkaTopics.trim.length <= 0){
      System.err.println("Usage: KafkaTopicStreaming <kafka_topic_index> is number ...")
      System.exit(1)
    }
    val topics: Set[String] = kafkaTopics.split(",").toSet[String]

    // TODO: iii. 连接Zookeeper Client
    // public ZkClient(zkServers: String, sessionTimeout: Int, connectionTimeout: Int, zkSerializer: ZkSerializer) {
    val zkClient = new ZkClient(
      props.getProperty("zk.connect"), // zkServers
      Integer.MAX_VALUE, // sessionTimeout
      100000, // connectionTimeout
      ZKStringSerializer
    )

    // TODO: iv. 消费组的名称
    val groupName = props.getProperty("group.id")

    /**
      * TODO： 2 -> 采用Direct方式从Kafka的Topic中读取数据，自己管理偏移量
      */

    // a. 读取配置文件中连接KAFKA及获取Topic相关参数信息
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> props.getProperty("metadata.broker.list"), //
      "auto.offset.reset" -> "largest", //
      "group.id" -> groupName
    )

    // b. 组装OFFSET，一个Topic有多个分区，每个分区都有自己的消费OFFSET，所以是一个Map集合
    import scala.collection.mutable
    val fromOffsets: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()
    // 使用循环对多个Topic中每个分区进行获取OFFSET
    topics.foreach(topic => {
      // b.1. 从Zookeeper上获取每个Topic的分区数
      val children: Int = zkClient.countChildren(ZkUtils.getTopicPartitionsPath(topic))
      // b.2. 依据Topic的分区，获取在Zookeeper上consumer的ZNODE的OFFSET
      for(partitionId <- 0 until children){
        // 构建一个Topic和Partition实例对象
        val tp = TopicAndPartition(topic, partitionId)
        // 构建路径path，基于groupName，topic使用ZKGroupTopicDirs工具类，获取到某个分区在Zookeeper上OFFSET存储ZNODE路径
        val path = s"${new ZKGroupTopicDirs(groupName, topic).consumerOffsetDir}/$partitionId"
        // 判断路径是否存在
        if(zkClient.exists(path)){
          // 如果路径存在，直接给获取路径ZNODE上的数据
          fromOffsets += tp -> zkClient.readData[String](path).toLong
        }else{
          // 如果不存在表示当前消费组中从未消费此分区的数据
          fromOffsets += tp -> 0L
        }
      }
    })
    // 打印偏移量相关信息
    logger.warn(s"++++++++++++++++++++++ fromOffsets: $fromOffsets ++++++++++++++++++++++")

    // c. 表示从KAFKA Topic中获取媒体数据 以后处理仿式，此处获取Topic名称和每条信息
    val  messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message())
    // d. 管理OFFSET，使用Direct方式从KAFKA TOPIC中读取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, // StreamingContext
        kafkaParams, // Map[String, String]
        fromOffsets.toMap, // Map[TopicAndPartition, Long]
        messageHandler //  MessageAndMetadata[K, V] => R
      )

    // kafkaDStream.print(10)

    // TODO: 创建KafkaCluster实例对象
    val kc =  new KafkaCluster(kafkaParams)

    /**
      * TODO: 3 -> 将获取的数据，插入到HBase表中
      */
    // kafkaDStream 直接从Kafka Topic中获取的数据，KafkaRDD就是直接获取的Topic的数据，未进行任何处理
    kafkaDStream.foreachRDD(rdd => {
      // =========================================================================
      if(!rdd.isEmpty()){
        // 获取当前批次RDD中各个分区数据的偏移量范围（KAFKA Topic中各个分区数据对应到RDD中各个分区的数据）
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // TODO: 基于RDD的每个分区进行操作，将数据批量存储到HBase表中，使用Put方式
        rdd.foreachPartition(iter => {
          // 获取当前分区ID（RDD中每个分区的数据被一个Task处理，每个Task处理数据的时候有一个上下文对象TaskContext）
          val partitionId: Int = TaskContext.getPartitionId()
          // 依据分区ID, 获取到分数据中对应Kafka Topic中分区数据的偏移量
          val offsetRange = offsetRanges(partitionId)
          logger.warn(s"${offsetRange.topic} - ${offsetRange.partition}: from [${offsetRange.fromOffset}], to [${offsetRange.untilOffset}]")

          // 插入数据到HBase之前，判断topic是否有值，表的名称与topic名称相同
          var isInsertSuccess = false
          if(null != offsetRange.topic){
            isInsertSuccess = HBaseDao.insert(offsetRange.topic, iter)
          }

/*
          if(null != offsetRange.topic){
            iter.foreach(item => {
              // 一条一条插入数据至HBase，性能很低下
            })
          }
*/
          // 当将分区的数据插入到HBase表中成功以后，更新Zookeeper上消费者偏移量
          if(isInsertSuccess){
            // 构建分区与偏移量对象实例
            val tp = TopicAndPartition(offsetRange.topic, offsetRange.partition)
            // 更新Zookeeper上消费偏移数据
            val either: Either[Err, Map[TopicAndPartition, Short]] = kc.setConsumerOffsets(
              groupName, // groupId
              Map(tp -> offsetRange.untilOffset) //
            )
            // 如果返回Left实例对象，说明插入异常
            if(either.isLeft){
              logger.error(s"Error updating the offset to Kafka Cluster ${either.left.get}")
            }
          }
        })
      // 每批RDD数据总的数目
      logger.warn(s"-------------------- Batch RDD Count = ${rdd.count()} --------------------")
      }
      // =========================================================================
    })

    /**
      * TODO: 4 -> 启动流式应用
      */
    ssc.start()
    ssc.awaitTermination()

    // 流式应用的停止
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
