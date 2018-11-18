package com.erongda.bigdata.meituantest.product

import com.erongda.bigdata.jedis.JedisPoolUtil
import com.erongda.bigdata.meituantest.util._
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * 广告点击流量实时统计分析: 对收集得到的用户点击广告数据进行数据分析，并将结果保存到Redis/MySQL中
  *   - 用户点击广告数据存储: Kafka
  *   -  数据处理方式: SparkStreaming
  *   - 结果保存: Redis/MySQL
  */
object AdClickRealTimeStateSpark {

  // Streaming批次时间间隔
  val BATCH_INTERVAL: Int = 3 // 30
  // 窗口大小，窗口范围为 10分钟
  val BLACK_LIST_WINDOW_INTERVAL: Int = BATCH_INTERVAL * 2 * 10
  // 批次/执行间隔大小（滑动窗口大小），批次产生的时间间隔为 3分钟
  val BLACK_LIST_SLIDER_INTERVAL: Int = BATCH_INTERVAL * 2 * 3

  // 数据分割符
  val delimeter: String = " "
  // 是否是本地执行，初始化设置，具体从配置文件中读取信息
  var isLocal: Boolean = false

  // Redis中黑名单的Key名称
  val REDIS_KEY_USER_BLACK = "ad:user:black"
  val REDIS_KEY_USER_WHITE = "ad:user:white"

  def main(args: Array[String]): Unit = {

    // TODO：一、 创建上下文
    val (sc, ssc) = {
      // a. 获取相关环境类别
      isLocal = ConfigurationManager.getBoolean(TrackConstants.SPARK_LOCAL)
      val appName = TrackConstants.SPARK_APP_NAME_AD

      // b. 获取SparkConf实例对象，配置应用属性
      val sparkConf = SparkConfUtils.generateSparkConf(appName, isLocal)

      // c. 构建SparkContext实例对象
      val sparkContext = SparkContextUtils.getSparkContext(sparkConf)
      // 关闭日志级别
      sparkContext.setLogLevel("WARN")

      // d. 创建StreamingContext, 设置Batch批次处理时间间隔BatchInterval
      val streamingContext = new StreamingContext(sparkContext, Seconds(BATCH_INTERVAL))

      // e. 由于程序中绘使用updateStateByKey API，需要状态的保存，所以设置checkpoint目录
      val path = "/datas/sparkstreaming/checkpoint/AdClickRealTimeStateSpark"
      // 当Streaming应用第一次运行的时候，先检查目录是否存在，如存在就删除
      FileSystem.get(sparkContext.hadoopConfiguration).delete(new Path(path), true)
      streamingContext.checkpoint(path)

      // f. 返回
      (sparkContext, streamingContext)
    }

    // TODO：二、Kafka集成形成DStream
    val kafkaDStream: InputDStream[(String, String)] = {
      // Kafka configuration parameters
      val kafkaParams: Map[String, String] = Map(
        "metadata.broker.list" -> ConfigurationManager.getProperty(TrackConstants.METADATA_BROKER_LIST),
        "auto.offset.reset" -> ConfigurationManager.getProperty(TrackConstants.AUTO_OFFSET_RESET)
      )
      // Names of the topics to consume
      val  topics: Set[String] = ConfigurationManager
        .getProperty(TrackConstants.TOPIC_NAMES).split(",").toSet[String]
      // 从Kafka Topic中获取数据，返回（key, value)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }
    // kafkaDStream.print(5)

    // TODO: 三、数据格式转换
    /*
      在真实项目中，这一部分代码可能会比较多，因为Kafka中的数据肯还需要进行必要的格式转换
          (25,1540538414070 province_38 city_138 9196186 2840)
       此处，将Message中Value转换封装到Case Class中即可
     */
    val adClickDStream: DStream[AdClickRecord] = kafkaDStream.transform(rdd => {
      rdd.map(message => {
        // a. 获取Topic中Message的Value值，再按照给定的分隔符进行数据分割（数据必须都不为空）
        val arrs: Array[String] = message._2.split(delimeter).map(_.trim).filter(_.nonEmpty)
        // b. 对符合数据格式要的数据进行数据转换操作
        var adClick: AdClickRecord = null
        if(arrs.length == 5 && message._2.nonEmpty){
          // TODO: 在实际开发中，可能在这里一些必要的转换操作，主要依据业务来定
          adClick = Some(AdClickRecord(arrs(0).toLong, arrs(1), arrs(2), arrs(3).toInt, arrs(4).toInt)).get
        }
        adClick
      })
      // TODO: 考虑优化？？ 当RDD的map、filter、flatMap连续存在的时候，最好换成只调用一个API
      /* 思考题：
      rdd.map(msg => msg._2)
        .filter(value => {
          null != value && value.trim.length > 0 && value.trim.split(delimeter).length == 5
        }).flatMap(value => value.split(delimeter))
      */
    })
    // adClickDStream.print(5)

    // TODO：四、黑名单的更新操作
    dynamicUpdateBlackList(adClickDStream)

    // TODO：五、过滤黑名单用户数据
    val filterAdClickDStream: DStream[AdClickRecord] = filterByBlackList(adClickDStream)
    // filterAdClickDStream.transform(rdd => rdd.map(_.userId).distinct()).print(50)


    // TODO：六、实时累加广告点击量
//    calculateRealTimeState(filterAdClickDStream)

    // 七、获取各个省份Top5的累加广告点击量结果
    /*
       输出的表结构：
        * 名称: tb_top5_province_ad_click_count
        * 字段：
          * date 日期
          * province 城市
          * ad_id 广告id
          * click_count 点击次数
        * 插入方式：Insert Or Update
    */
    // 八、分析最近一段时间广告流量点击情况
    /*
      实时统计最近10分钟的某个广告点击数量
        * -1. 窗口大小
          * window interval： 10 * 60 = 600s
        * -2. 执行批次
          * slider interval： 1 * 60 = 60s
      数据结果保存
        * 表名称: tb_ad_click_count_of_window
        * 字段：
          * date: 时间格式字符串
          * ad_id: 广告点击id
          * click_count：点击基础
        * 数据插入方式：Insert Or Error
    */

    // 九、启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()

    // 十、关闭Stremaing应用
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  /**
    * 基于用户点击广告数据进行黑名单更新操作，将黑名单数据写入到Redis中
    * @param adDStream
    *                  广告点击流
    */
  def dynamicUpdateBlackList(adDStream: DStream[AdClickRecord]): Unit ={
    /**
      * a. 规则：
      *   - 最近10分钟用户广告点击次数超过100次（某个用户对所有广告点击次数）
      *   - 黑名单列表每隔3分钟更新一次
      *   - 如果一个用户被添加到黑名单中，在程序判断中，该用户永远都是黑名单用户；除非工作人员干预，手动删除该用户的黑名单标记
      *   - 支持白名单（白名单中的用户不管点击多少次，都不算是黑名单中存在的）
      * b. 使用DStream的窗口分析函数进行数据更新操作
      *   - 统计用户点击广告次数
      *     次数统计规则：一条数据就是点击一次
      *   - 过滤少于100的数据
      *   - 支持白名单用户过滤
      * c. 白名单用户的数据有工作人员手动添加到Redis中，过滤过程中只需读取即可
      *   优化点：在Streaming应用启动之前从Redis中读取白名单数据，缓存起来，供后期使用
      * d. 无论是黑名单还是白名单
      *     数据量都是很少的，需要实时读取及写入，此处建议使用内存数据库，比如Key/Value类型Redis数据库
      */
    // TODO: 针对DStream开发来说，秉着可以对RDD操作的，就不对DStream操作原则
    adDStream
      // 设置窗口大小： 10分钟； 滑动大小： 3分钟
      .window(Seconds(BLACK_LIST_WINDOW_INTERVAL), Seconds(BLACK_LIST_SLIDER_INTERVAL))
      .foreachRDD(rdd => {
        // ====================================================================
        if(!rdd.isEmpty()){
          // RDD 数据缓存
          rdd.persist(StorageLevel.MEMORY_AND_DISK)

          // 构建SparkSession实例对象
          val spark: SparkSession = SparkSession.builder()
            .config(rdd.sparkContext.getConf)
            .config("spark.sql.shuffle.parititons", "8")
            .getOrCreate()
          import spark.implicits._

          /**
            * TODO： 作业 -> 自己实现读取Redis中白名单的数据
            *   使用模拟数据，数据量不大，主要包含用户ID => 可以使用广播变量的形式将广播出去进行过滤
            */
          // 模拟数据：白名单的数据
          val sc: SparkContext = spark.sparkContext
          val whiteListRDD: RDD[Int] = sc.parallelize(1 to 10)
          // 将白名单数据广播出去
          val broadcastOfWhiteList: Broadcast[Array[Int]] = sc.broadcast(whiteListRDD.collect())

          // 将窗口内数据RDD转换为DataFrame，按照各个用户ID进行分组，统计每个用户点击广告次数
          rdd //
            .toDS() // 由于窗口较大（10分钟），数量较大，此处集成SparkSQL分析（采用DSL）
            .select($"userId", $"adId").groupBy($"userId").count() // 分组统计
            .filter($"count" > 100).rdd.coalesce(1) // 转换为RDD并降低分区数
            // 采用调用RDD中foreachPartition API 进行输出操作
            .foreachPartition((iter: Iterator[Row]) => {
            Try{
              // 获取 Jedis连接
              val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
              // 先过滤白名单用户，再插入Redis中
              iter.foreach{ case Row(userId: Int, _: Long) =>
                // 使用白名单过滤
                if(!broadcastOfWhiteList.value.contains(userId)){
                  // 将黑名单插入Redis的set无序集合中
                  jedis.sadd(REDIS_KEY_USER_BLACK, userId.toString)
                }
              }
              // 返回
              jedis
            }match {
              case Success(jedisValue) => JedisPoolUtil.release(jedisValue)
              case Failure(exception) => throw exception
            }
          })

          // 释放缓存
          rdd.unpersist(true)
        }
        // ====================================================================
      })
  }

  /**
    * 根据Redis中黑名单进行数据过滤操作
    * @param adDStream
    *                  广告点击流数据
    * @return
    */
  def filterByBlackList(adDStream: DStream[AdClickRecord]): DStream[AdClickRecord] = {
    // 将DStream数据的过滤操作转换为RDD数据的过滤
    adDStream.transform(rdd => {
      // i. 从Redis中读取黑名单数据
      val blackListUsers: List[String] = {
        Try{
          // 获取 Jedis连接
          val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
          // 从Redis中依据Key读取黑名单用户
          import scala.collection.JavaConverters._
          val blackSetUsers: mutable.Set[String] = jedis.smembers(REDIS_KEY_USER_BLACK).asScala
          // 返回
          (jedis, blackSetUsers.toList)
        }match {
          case Success((jedisValue, blackUsers)) =>
            // 关闭连接
            JedisPoolUtil.release(jedisValue)
            // 返回
            blackUsers
          case Failure(exception) => throw exception
        }
      }

      // ii. 过滤黑名单中的数据（过滤分为Map端过滤 和Reduce过滤）
      /**
        * TODO：过滤方式具体技术点：
        *   第一、Map端的过滤：利用广播变量进行数据过滤
        *   第二、Reduce端的数据过滤：利用ledtOuterJoin后的RDD的数据进行过滤
        * TODO: 前面已经采用 广播变量的方式Map端过滤，下面演示Reduce端过滤，步骤如下；
        *   a. 将数据转换为Key/Value类型RDD，方便按照Key进行数据Join
        *   b. 调用leftOuterJoin/RightOuterJoin
        *   c. 调用filter过滤数据
        *   d. map数据转换
        */
      // 转换黑名单用户为RDD且数据类型为二元组
      val blackListRDD: RDD[(Int, Int)] = rdd.sparkContext
        .parallelize(blackListUsers.map(userId => (userId.toInt, 0)))
      // 采用leftOuterJoin过滤数据并返回
      rdd //
        .map(adRecord => (adRecord.userId, adRecord)) // 转换二元组
        .leftOuterJoin(blackListRDD) // 进行左外关联
        .filter{  // RDD[(Int, (AdClickRecord, Option[Int]))]
        case (_, (_, option)) =>
          // 当option为Some类型时候，表示当前useId为blackListRDD中出现了；否则表示没有出现
          // 最终期望结果是用户在黑名单中没有出现 => 要求option为None
          option.isEmpty
      }
        .map{ case (_, (record, _)) => record}
    })
  }

  /**
    * 实时累加统计各个广告点击流量：
        a. 维度信息：每天 每个省份 每个城市
        b. DStream[((date, 省份, 城市， 广告), 点击量)
    *
    * @param adDStream
    *                  过滤后的广告点击流量数据
    */
  /*def calculateRealTimeState(adDStream: DStream[AdClickRecord]): Unit = {
    // 1. 将adDStream转换为Key/Value对形式
    val mappedDStream: DStream[((String, String, String, Int), Int)] = adDStream.transform(rdd => {
      rdd.map{ case AdClickRecord(timestamp, province, city, userId, adId) =>
        // 1.a. 根据timestamp获取时间格式字符串，格式为:yyyyMMdd
        val date = DateUtils.parseLong2String(timestamp, "yyyyMMdd")
        // 1.b. 返回结果
        ((date, province, city, adId), 1)
      }
    })

    // 2. 累加计算结果值  => 考虑使用updateStateByKey API实现
    /**
      * 方式一：updateStateByKey：
      *     随着数据规模的执行时间延长，结果数据会越来越长，对性能会有一定影响
      *       - 某些不会出现的Key， updateSateByKey会进行保存
      *       - 可以通过返回None的形式，表示不缓存该Key的数据
      * 方式二：mapWithState (推荐使用）
      *     可以缓解updateStateByKey API的问题
      */
    val adTop5Count: DStream[((String, String, String, Int), Int)] = mappedDStream.transform(rdd => {
      rdd.reduceByKey(_ + _)
    })
      .updateStateByKey(
        (values: Seq[Int], state: Option[Int]) => {
          // 获取当前批次中Key的状态（总的订单销售额）
          val currentTotal = values.sum
          state match {
            case Some(adTotal) => Some(adTotal + currentTotal)
            case None => Some(currentTotal)
          }
        })

    null
  }*/
}





/**
  * 封装广告点击的数据
  * @param timestamp
  *                  时间戳，毫秒级别时间戳，long类型
  * @param province
  *                 省份名称
  * @param city
  *             城市名称
  * @param userId
  *               用户ID
  * @param adId
  *             广告ID
  */
case class AdClickRecord(timestamp: Long, province: String,
                         city: String, userId: Int, adId: Int)
