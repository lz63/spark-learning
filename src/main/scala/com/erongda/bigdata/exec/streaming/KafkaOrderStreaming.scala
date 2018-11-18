package com.erongda.bigdata.exec.streaming

import java.util.Properties

import com.erongda.bigdata.jedis.JedisPoolUtil
import com.erongda.bigdata.spark.streaming.Order
import com.erongda.bigdata.utils.ConstantUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * SparkStreaming实时从Kafka Topic中读取数据（仿双十一订单数据分析）：
  * -a. 实时累加统计 各省份销售订单额
  * updateStateByKey   -> Redis中（使用哈希数据类型）
  * -b. 统计最近20秒各个省份订单量 -> BatchInterval: 4 秒， slide：8秒
  * SparkSQL（SQL）分析 -> 数据库中
  * 联动测试，每批次处理数据量最多为
  * 8000 * 3 * 4 = 96000 条
  */

object KafkaOrderStreaming {

  // Redis 数据库中Key的值
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "order:total:price"

  /**
    * 用于创建StreamingContext实例对象，读取流式数据，实时处理与输出
    *
    * @param args
    * 程序的参数
    * @param operation
    * 读取流式数据、实时处理与结果输出地方
    */
  def sparkOperation(args: Array[String])(operation: StreamingContext => Unit): Unit = {

    // 判断传递的参数，设置Spark Application运行的地方
    if (args.length < 3) {
      println("Usage: KafkaOrderStreaming <master> <batchInterval> <maxRatePerPartition>")
      System.exit(0)
    }

    var context: StreamingContext = null
    try {
      // 创建StreamingContext实例对象
      context = StreamingContext.getActiveOrCreate(
        ConstantUtils.CHECK_POINT_DIRECTORY, // 非第一次运行流式应用，重启从检查点目录构建StreamingContext
        () => {
          // 第一次运行流式应用 ，创建StreamingContext
          // i. Spark Application配置
          val sparkConf = new SparkConf()
            .setAppName("KafkaOrderStreaming")
            .setMaster(args(0))
            .set("spark.streaming.kafka.maxRatePerPartition", args(2))
            // 设置序列化
            .registerKryoClasses(Array(classOf[Order]))
          // 设置批处理时间间隔batchInterval
          val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))
          // 设置检查点目录
          ssc.checkpoint(ConstantUtils.CHECK_POINT_DIRECTORY)

          // 真正处理流式数据
          operation(ssc)

          // 返回StreamingContext实例对象
          ssc
        }
      )
      context.sparkContext.setLogLevel("WARN")
      // 启动Streaming应用
      context.start()
      context.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != context) context.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  /**
    * SparkStreaming从Kafka接收数据，依据业务实时处理分析，将结果输出到Redis内存数据库
    *
    * @param ssc
    * SteamingContext实例对象
    */
  def processStreamingData(ssc: StreamingContext): Unit = {
    // TODO 1. 从Kafka中读物数据，采用Direct方式
    // Kafka Cluster连接配置相关参数
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> ConstantUtils.METADATA_BROKER_LIST,
      "auto.offset.reset" -> "largest"

    )
    // Kafka中Topic的名称
    val topics: Set[String] = Set(ConstantUtils.ORDER_TOPIC)
    // 从Kafka Topic获取数据，返回(key, message)
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, // StreamingContext
      kafkaParams, // Map[String, String]
      topics // Set[String]
    )
    //    kafkaDStream.print(5)
    val orderDStream: DStream[(Int, Order)] = kafkaDStream.transform(rdd => {
      rdd
        .filter(line => line._2.trim.length > 0)
        .map(item => {
          // TODO: 优化：对ObjectMapper类进行单例模式，只创建一次，省资源
          val mapper = ObjectMapperSingleton.getInstance()
          val order = mapper.readValue(item._2, classOf[Order])
          (order.provinceId, order)
        })
    })
//    orderDStream.print(5)

    // TODO 2.实时累加统计销售订单额，调用DStream#updateStateByKey
    val orderTotalDStream: DStream[(Int, Double)] = orderDStream
      // 转换读取到Message，返回二元组（provinceId, orderPrice)
      .transform(rdd => {
      rdd
        .map(line => (line._2.provinceId,line._2.orderPrice))
    })
      // 实时累加更新函数： updateFunc: (Seq[V], Option[S]) => Option[S]
      .updateStateByKey(
      (values: Seq[Double], state: Option[Double]) => {
        // 获取当前批次中Key的状态（总的订单销售额）
        val currentTotal = values.sum
        state match {
          case Some(previousTotal) => Some(previousTotal + currentTotal)
          case None => Some(currentTotal)
        }
      }
    )
    //  Typically, a checkpoint interval of 5 - 10 sliding intervals of a DStream is a good setting to try.
    orderTotalDStream.checkpoint(Seconds(8))
    orderTotalDStream.print(5)

    // TODO 3. 将实时累加销售订单额存储到Redis内存数据库中
    // orderTotalDStream.print()
    orderTotalDStream.foreachRDD(rdd => {
      // 判断当前批次输出RDD是否有数据
      if (!rdd.isEmpty()) {
        // 将RDD保存Redis数据库中，降低分区数, 对RDD的每个分区进行操作
        rdd.coalesce(3).foreachPartition(iter => {
          // 将结果写入Redis数据库中
          var jedisPool: JedisPool = null
          var jedis: Jedis = null
          try {
            // i. 获取连接池实例对象
            jedisPool = JedisPoolUtil.getJedisPoolInstance
            // ii. 获取Jedis连接
            jedis = jedisPool.getResource
            // iii. 将统计销售额数据存储到Redis中
            iter.foreach {
              case (provinceId, orderTotal) =>
                jedis.hset(REDIS_KEY_ORDERS_TOTAL_PRICE, provinceId.toString, orderTotal.toString)
            }
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            // iv. 关闭连接
            JedisPoolUtil.release(jedis)
          }
        })
      }
    })
    // TODO 4. 统计最近20秒各个省份订单量存储到MySQL中

    val schema = StructType(
      StructField("orderID", StringType, nullable = false) ::
        StructField("provinceId", IntegerType, nullable = false) ::
        StructField("orderPrice", DoubleType, nullable = false) :: Nil
    )
    orderDStream.window(Seconds(20), Seconds(8)).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
//        val rowRDD = rdd.map(line => Row(line._1, line._2, line._3))
//        val orderDF: DataFrame = spark.createDataFrame(rowRDD, schema)
//        orderDF.printSchema()
//        orderDF.first()
//        rdd.toDS()
        val spark = SparkSession
          .builder()
          .config(rdd.sparkContext.getConf)
          .config("spark.sql.shuffle.partitions", "20")
          .getOrCreate()
        import spark.implicits._
        val orderDS = rdd.toDS()
        orderDS.printSchema()
        orderDS.show(5, truncate = false)
        orderDS.createOrReplaceTempView("view_tmp_order")
        val orderSQL = spark.sql(
          """
            |SELECT provinceId,COUNT(1) FROM view_tmp_order GROUP BY provinceId
          """.stripMargin)
        orderSQL.printSchema()
        orderSQL.write.mode(SaveMode.Overwrite).jdbc(
          "jdbc:mysql://bigdata-training01.erongda.com:3306/spark_db?user=root&password=123456",
          "order_tb",
          new Properties())
      }
    })
  }

  /**
    * 实时累加统计 各省份销售订单额
    *         updateStateByKey   -> Redis中（使用哈希数据类型）
    * @param orderDStream
    *                     DStream流
    */
  def realTimeOrderTotalState(orderDStream: DStream[(Int, Order)]): Unit ={
    orderDStream
      // 转换读取到Message，返回二元组（provinceId, orderPrice)
      .transform(rdd => {
      rdd
        .map(line => (line._2.provinceId,line._2.orderPrice))
    })
      // 实时累加更新函数： updateFunc: (Seq[V], Option[S]) => Option[S]
      .updateStateByKey(
      (values: Seq[Double], state: Option[Double]) => {
        // 获取当前批次中Key的状态（总的订单销售额）
        val currentTotal = values.sum
        state match {
          case Some(previousTotal) => Some(previousTotal + currentTotal)
          case None => Some(currentTotal)
        }
      }
    )
  }

  def main(args: Array[String]): Unit = {
    //    orderProducer
    sparkOperation(args)(processStreamingData)
  }

}

/** Lazily instantiated singleton instance of SparkSession */
object ObjectMapperSingleton {
  // 使用transient注解标识的时候，表示此变量不会被序列化，ObjectMapper为被单例的类名
  @transient  private var instance: ObjectMapper = _

  def getInstance(): ObjectMapper = synchronized{
    if (instance == null) {
      instance = new ObjectMapper()
      instance.registerModule(DefaultScalaModule)
    }
    instance
  }
}

