package com.erongda.bigdata.exec.streaming

import java.util.{Properties, Random, UUID}

import com.erongda.bigdata.spark.streaming.Order
import com.erongda.bigdata.utils.ConstantUtils
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  * 模拟生产订单数据，发送到Kafka Topic中
  *     Topic中每条数据Message类型为String，以JSON格式数据发送
  * 数据转换：
  *     将Order类实例对象转换为JSON格式字符串数据（可以使用fastjson类库）
  *     https://github.com/alibaba/fastjson
  */
object OrderProducer {
  def main(args: Array[String]): Unit = {
    /**
      * 构建一个Kafka Producer 生产者实例对象，以便向Topic发送数据
      */
    // def this(config: ProducerConfig)，所以需要ProducerConfig
    //  def this(originalProps: Properties)，所以需要Properties
    var producer: Producer[String, String] = null
    // 创建Properties实例对象
    val props: Properties = new Properties()
    // 设置broker节点
    props.put("metadata.broker.list", ConstantUtils.METADATA_BROKER_LIST)
    // 设置序列化类的类型
    props.put("serializer.class", ConstantUtils.SERIALIZER_CLASS)
    // 设置key序列化类型
    props.put("key.serializer.class", ConstantUtils.SERIALIZER_CLASS)
    // 设置发送数据为异步发送，此时数据发送很多
    props.setProperty("producer.type", "async")
    // 创建ProducerConfig实例对象并将properties参数传入
    val config: ProducerConfig = new ProducerConfig(props)
    // TODO: 模拟的时候，每次想Topic发送多条数据，需要列表存储
    val messageArrayBuffer = new ArrayBuffer[KeyedMessage[String, String]]()
    try {
      // 实例化生产者对象，传入参数config
      producer = new Producer[String, String](config)
      while (true) {
        // 清空
        messageArrayBuffer.clear()
        // 用于产生随机数
        val random = RandomSingleton.getInstance() //
        // 每次循环 模拟产生的订单数目
        val randomNumber = random.nextInt(100) + 5
        // TODO: startTime
        val startTime = System.currentTimeMillis()
        // TODO: 使用FOR循环产生订单数据
        for(index <- 0 to randomNumber){
          // 模拟创建订单实例对象
          val orderId = UUID.randomUUID().toString //订单ID
          val provinceId = random.nextInt(34) + 1  // 省份ID
          val orderPrice = random.nextInt(100) + 25.9  // 订单价格
          val order = Order(orderId, provinceId, orderPrice)

          val mapper = ObjectMapperSingleton.getInstance()
          mapper.registerModule(DefaultScalaModule)
          // 构建keyedMessage
          val message = new KeyedMessage[String, String](
            ConstantUtils.ORDER_TOPIC,  // topic
            orderId,  // key
            mapper.writeValueAsString(order)  // value
          )
          // 添加到数组中
          messageArrayBuffer += message
        }
        // TODO: def send(messages: KeyedMessage[K,V]*), 此处批量将数据发送到Topic
        producer.send(messageArrayBuffer: _*)
        // TODO: endTime
        val endTime = System.currentTimeMillis()
        //      println(messageArrayBuffer: _*)
        println(s"--------------- Send Messages: $randomNumber, Spent Time: ${endTime - startTime}")
      }

    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if (null != producer) producer.close()
    }
  }
}

/** Lazily instantiated singleton instance of SparkSession */
object RandomSingleton {

  @transient  private var instance: Random = _

  def getInstance(): Random = synchronized{
    if (instance == null) {
      instance = new Random
    }
    instance
  }
}




