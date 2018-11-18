package com.erongda.bigdata.meituantest.product

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.erongda.bigdata.jedis.JedisPoolUtil
import com.erongda.bigdata.meituantest.dao.JDBCHelper
import com.erongda.bigdata.spark.streaming.Order
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.util.{Failure, Success, Try}

/**
  * Created by fuyun on 2018/10/25.
  */
object AdClickDataTop5 {

  // 设置Streaming Application检查点目录
  val CHECK_POINT_DIRECTORY = "/datas/spark/streaming/ktrs-ckpt-0000000000" + System.currentTimeMillis()

  // Redis 黑名单数据库中Key的值
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "ad:uder:blacklist"

  val METADATA_BROKER_LIST_NAME = "metadata.broker.list"

  val METADATA_BROKER_LIST_VALUE = "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094"

  /**
    * TODO: 贷出模式中贷出函数loan function：获取资源及关闭资源，调用用户函数处理业务数据
    * @param args
    *             程序的参数
    * @param operation
    *                  用户函数，此处为实时接收流式数据并进行处理与输出
    */
  def sparkOperation(args: Array[String])(operation: (StreamingContext, Int) => Unit): Unit = {

    // 判断传递一个参数，设置Spark Application运行的地方
    if(args.length < 3){
      println("Usage: SparkStreamingModule <master> <BatchInterval> <maxRatePerPartition>.......")
      System.exit(1)
    }

    // TODO: 一、 创建上下文
    var context: StreamingContext = null
    try{
      // 创建StreamingContext实例对象，考虑高可用性，检查点
      context = StreamingContext.getActiveOrCreate(
        CHECK_POINT_DIRECTORY, //
        () => { //
        // 创建SparkConf实例对象，设置应用相关信息
        val sparkConf = new SparkConf()
          .setMaster(args(0))
          .setAppName("AdClickDataTop5")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.streaming.kafka.maxRatePerPartition", args(2))
          // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
          val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))
          // 设置日志级别
          ssc.sparkContext.setLogLevel("WARN")

          // 设置检查点目录
          ssc.checkpoint(CHECK_POINT_DIRECTORY)
          // TODO：从KAFKA Topic中实时去读数据，进行分析处理，并打印控制台
          operation(ssc, args(1).toInt)

          // 返回StreamingContext实例对象
          ssc
        }
      )
      // 设置日志级别
//      context.sparkContext.setLogLevel("WARN")
      // 启动流式应用
      context.start()
      context.awaitTermination()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      // 关闭StreamingContext
      if(null != context) context.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  /**
    * TODO: 贷出模式中用户函数（user function)
    * @param ssc
    *            StreamingContext实例对象，读取数据源的数据
    */
  def processStreamingData(ssc: StreamingContext, interval: Int): Unit ={
    // 从kAFkA中读取数据的相关配置信息上的设置
    val kafkaParams: Map[String, String] = Map(
      METADATA_BROKER_LIST_NAME -> METADATA_BROKER_LIST_VALUE,
      "auto.offset.reset" -> "largest"
    )
    // 设置从哪些Topic中读取数据，可以是多个Topic
    val topics: Set[String] = Set("adTopic")
    // TODO: 二、采用Direct方式从Kafka Topic中pull拉取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, //
      kafkaParams, //
      topics
    )

    // TODO：2. 调用DStream中API对数据分析处理
    /**
      * Kafka Topic中每条Message数据格式：
      * 字段信息：timestamp,province,city,userid,adid
      */
    val adDStream: DStream[(String, Ad)] = kafkaDStream
      .map(_._2)
      .transform(rdd => {
        rdd
          // 表示过滤不合格的数据
          .filter(line => line.trim.length > 0 && line.trim.split(" ").length >= 5)
          // 提取字段信息：provinceId,orderPrice
          .map(line => {
            val Array(timeStamp, provinceName, cityName, userId, adId) = line.split(" ")
            // 将时间转换为Long类型
            val time = timeStamp.toLong
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dateTime: String = sdf.format(new Date(time))
            (dateTime, Ad(dateTime, provinceName, cityName, userId, adId))
          })
      })

    // 因为后面常用，将其缓存
//    adDStream.persist(StorageLevel.MEMORY_AND_DISK)
    // 输出测试
    /*adDStream.foreachRDD((rdd: RDD[(Long, Ad)], time: Time) => {
      println("-----------------------------------------")
      val batchTime = time.milliseconds
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      val batchDateTime = sdf.format(new Date(batchTime))
      println(s"Batch Time: $batchDateTime")
      println("-----------------------------------------")
      if(!rdd.isEmpty()){
        // TODO: RDD输出至关重要，往往先降低RDD分区数，在对每个分区数据进行输出操作
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
    })*/

    // TODO: 黑名单的更新操作
    /**
      * 黑名单机制   -> Redis数据库：数据结构list列表
      * 存在一些恶意点击广告的用户，不希望统计这些恶意用户的点击情况，广告流量统计中需要将黑名单用户的数据过滤掉。
			* 黑名单用户定义规则：最近一段时间(最近10分钟)，总广告点击次数超过100次的用户就认为是黑名单用户。
			* 规则：
			* 	其一、黑名单列表每隔3分钟更新一次
			* 	其二、如果一个用户被添加到黑名单中，在程序判断中，该用户永远是黑名单用户；除非工作人员人工干预，手动删除该用户黑名单的标记
			* 	其三、支持白名单(白名单中的用户不管点击多少次，都不算是黑名单中存在的)
      */

    // 窗口统计, 设置窗口大小
    // val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(interval) * 3)
    val windowDStream: DStream[(String, Ad)] = adDStream
      .window(Seconds(interval) * 10, Seconds(interval) * 3)

    // 白名单列表
    val whiteList = List("1953171", "9042667")

    // 统计出黑名单用户并保存到Redis
    windowDStream.foreachRDD(rdd => {
      // 当rdd不为空时
      if(!rdd.isEmpty()) {
        // 得到黑名单用户ID和点击次数
        val blackUserlist = rdd.map(tuple => {
          // 转换为用户ID的二元组
          (tuple._2.userId, 1)
        })
          // 根据用户ID聚合统计点击次数
          .reduceByKey(_ + _)
          // 过滤点击次数大于100的用户ID并且不是在白名单中
          .filter(item => {
            item._2 >= 100 && !whiteList.contains(item._1)
          })
        println("=========测试输出==========")
        // 测试输出
        blackUserlist.coalesce(1).foreachPartition(_.foreach(println))

        // TODO: RDD输出至关重要，往往先降低RDD分区数，在对每个分区数据进行输出操作
        blackUserlist.coalesce(1).foreachPartition(iter => {
          // 将结果写入Redis数据库中
          var jedisPool: JedisPool = null
          var jedis: Jedis = null
          try{
            // i. 获取连接池实例对象
            jedisPool = JedisPoolUtil.getJedisPoolInstance
            // ii. 获取Jedis连接
            jedis = jedisPool.getResource
            // iii. 将统计销售额数据存储到Redis中
            iter.foreach{
              case (userId, adClickTotal) =>
                jedis.sadd(REDIS_KEY_ORDERS_TOTAL_PRICE, userId)
//                println(jedis.smembers(REDIS_KEY_ORDERS_TOTAL_PRICE))
            }
            println("==========已写入Redis中============")
          }catch {
            case e: Exception => e.printStackTrace()
          }finally {
            // iv. 关闭连接
            JedisPoolUtil.release(jedis)
          }
        })
      }
    })


    // TODO: 过滤黑名单用户数据

    // 获取过滤后的用户数据
    val filterAdDStream = adDStream.transform(rdd => {
      rdd.filter(item => {
        import scala.collection.mutable
        var blackSet: mutable.Set[String] = null
//        var blackSet: util.Set[String] = null
        var jedisPool: JedisPool = null
        var jedis: Jedis = null
        try{
          // i. 获取连接池实例对象
          jedisPool = JedisPoolUtil.getJedisPoolInstance
          // ii. 获取Jedis连接
          jedis = jedisPool.getResource
          // iii. 将统计销售额数据存储到Redis中
          import scala.collection.JavaConverters._
//          blackSet = jedis.smembers(REDIS_KEY_ORDERS_TOTAL_PRICE)
          blackSet = jedis.smembers(REDIS_KEY_ORDERS_TOTAL_PRICE).asScala
        }catch {
          case e: Exception => e.printStackTrace()
        }finally {
          // iv. 关闭连接
          JedisPoolUtil.release(jedis)
        }
          !blackSet.contains(item._2.userId)
      })

    })

    // TODO: 六、实时累加广告点击量
    val adClickCount: MapWithStateDStream[String, Int, Int, (String, Int)] = filterAdDStream.transform(rdd => {
      rdd.map(item => {
        (s"${item._2.dateTime.substring(0, 10)}:${item._2.provinceName}:${item._2.adId}", 1)
      })
        .reduceByKey(_ + _)
    })
      .mapWithState(
        StateSpec.function(
          // mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
          (dataTimeProvinceNameAdId: String, valueOption: Option[Int], state: State[Int]) => {
            // i. 获取当前Key的状态信息
            val currentValue: Int = valueOption match {
              case Some(count) => count
              case None => 0
            }
            // ii. 获取当前Key以前的状态
            val previousValue = if(state.exists()) state.get() else 0
            // iii. 合并状态
            val lastestValue = currentValue + previousValue
            // iv. 更新状态
            state.update(lastestValue)
            // v. 返回
            (dataTimeProvinceNameAdId, lastestValue)
          }
        )
      )
    println("=============实时累加广告点击量=====================")
    adClickCount.print(10)

    // TODO: 获取各个省份Top5的累加广告点击量结果
    adClickCount.foreachRDD(rdd => {
      val spark = SparkSession.builder()
        .config(rdd.sparkContext.getConf)
        .config("spark.sql.shuffer.partitions", "6")
        .getOrCreate()

      import spark.implicits._
      // 得到rowRDD
      val rowRDD: RDD[Row] = rdd.map(item => {
        val Array(date, provinceName, adId) = item._1.split(":")
        Row(date, provinceName, adId.toInt, item._2.toInt)
      })
      // 创建schema
      val schema = StructType(
        Array(
          StructField("date", StringType, nullable = true),
          StructField("province_name", StringType, nullable = true),
          StructField("ad_id", IntegerType, nullable = true),
          StructField("click_count", IntegerType, nullable = true)
        )
      )
      // 通过自定义rowRDD和schema得到DataFrame
      val adCountDF = spark.createDataFrame(rowRDD, schema)
      // 创建视图
      adCountDF.createOrReplaceTempView("view_temp_ad_chick_count")
      // 通过SQL语句得到各省累计广告点击量TOP5
      val provinceAdClickTop5: DataFrame = spark.sql(
        """
          |select
          |  tmp.date, tmp.province_name, tmp.ad_id, tmp.click_count
          |from(
          |  select
          |    date, province_name, ad_id, click_count,
          |    row_number() over(partition by province_name order by click_count desc) as pc
          |  from
          |    view_temp_ad_chick_count) as tmp
          |where
          |  tmp.pc <= 5
        """.stripMargin)

      // 测试输出
      provinceAdClickTop5.show(5, truncate = false)
      /*provinceAdClickTop5.foreachPartition(iter => {
        Try{
          // i. 获取conncetion
          val conn: Connection = JDBCHelper.getMySQLConnection

          // 获取数据库原始事务设置
          val oldAutoCommit = conn.getAutoCommit
          // TODO: 设置事务，针对当前分区数据要么都插入数据库，要么都不插入
          conn.setAutoCommit(false)

          // ii. 创建PreparedStatement 实例对象
          // 插入数据的SQL语句
          /**
            * 此处要实现的功能：Insert or Update，针对MySQL数据库来说语法如下：
            *     INSERT INTO tableName(field1, field2, ...) VALUES (?, ?, ...) ON DUPLICATE KEY UPDATE field1=value1, field2=value2, ...
            */
          val sqlStr = "INSERT INTO tb_top5_province_ad_click_count(`date`, `province_name`, `ad_id`, `click_count`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `province_name` = VALUES(`province_name`), `count` = VALUES(`count`),`city_infos` = VALUES(`city_infos`)
          val pstmt: PreparedStatement =  conn.prepareStatement(sqlStr)

          // iii. 对数据进行迭代输出操作
          // 定义计数器，计数数据条目数
          var recordCount = 0
          iter.foreach(row => {
            val date = row.getAs[String]("date")
            val areaLevel = row.getAs[String]("area_level")
            val productId = row.getAs[String]("product_id")
            val cityInfos = row.getAs[String]("city_infos")
            val clickCount = row.getAs[Long]("click_count")
            val productName = row.getAs[String]("product_name")
            val productType = row.getAs[String]("product_type")

            // 设置参数
            pstmt.setLong(1, date)
            pstmt.setString(2, area)
            pstmt.setString(3, productId)
            pstmt.setString(4, areaLevel)
            pstmt.setLong(5, clickCount)
            pstmt.setString(6, cityInfos)
            pstmt.setString(7, productName)
            pstmt.setString(8, productType)

            // 添加批次
            pstmt.addBatch()
            recordCount += 1

            // 设置每批次提交的数据量为500，当达到500条数据的时候，提交执行一次
            if(recordCount % 500 == 0){
              pstmt.executeBatch()
              conn.commit()
            }
          })

          // iv. 进行批次提交事务，插入数据
          pstmt.executeBatch()
          // 手动提交事务，将批量数据要么全部插入，要么不全不插入失败
          conn.commit()

          // 返回值
          (oldAutoCommit, conn)
        }match {
          // 执行成功，没有异常
          case Success((oldAutoCommit, conn)) =>
            // 当数据插入成功到数据库以后，恢复原先数据库事务相关设置
            Try(conn.setAutoCommit(oldAutoCommit))
            if(null != conn) conn.close()
          case Failure(exception) => throw exception
        }
      })*/



    })




    // TODO: 分析最近一段时间广告流量点击情况








  }

















  def main(args: Array[String]): Unit = {


    // 调用贷出函数
    sparkOperation(args)(processStreamingData)





  }
}


case class Ad(dateTime: String, provinceName: String, cityName: String, userId: String, adId: String)