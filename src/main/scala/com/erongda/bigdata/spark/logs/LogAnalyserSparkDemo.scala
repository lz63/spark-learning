package com.erongda.bigdata.spark.logs

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用SparkCore对Apache Log进行分析
  */
object LogAnalyserSparkDemo {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf对象
    val sparkConf = new SparkConf()
      .setAppName("LogAnalyserSpark")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(sparkConf)
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    // 设置日志级别
    sc.setLogLevel("WARN")

    // TODO: 1. 读取数据
    val accessLogsRDD = sc.textFile(ContantUtils.LOCAL_DATA_DIC + "/access_log")
      .filter(line => ApacheAccessLog.isValidateLogLine(line))
      .map(line => {
        ApacheAccessLog.parseLogLine(line)
      })

    // 由于后续四个需求均对上述的RDD进行操作分析，可以将其缓存
    accessLogsRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"Count = ${accessLogsRDD.count()}\n First = ${accessLogsRDD.first()}")
    /**
      * 需求一：Content Size
      *     The average, min, and max content size of responses returned from the server
      */
    val contentSizeRDD = accessLogsRDD.map(_.contentSize)
    // 此RDD使用多次，需要缓存
    contentSizeRDD.persist(StorageLevel.MEMORY_AND_DISK)
    // compute
    val minContentSize = contentSizeRDD.min()
    val maxCountSize = contentSizeRDD.max()
    val avgCountSize: Double = contentSizeRDD.sum()/contentSizeRDD.count()

    // 释放内存
      contentSizeRDD.unpersist()
    // 打印结果
   println(s"MIN = ${minContentSize}\t MAX = ${maxCountSize} \t AVG = ${avgCountSize}")

    /**
      * 需求二：Response Code
      *     A count of response code's returned.
      */
    val responseCodeRDD = accessLogsRDD
      .map(item => (item.responseCode, 1))
      .reduceByKey(_ + _)
      .collect()

    println(s"Response Code ${responseCodeRDD.mkString(", ")}")
    /**
      * 需求三：IP Address
      *     All IP Addresses that have accessed this server more than N times.
      */
    import spark.implicits._
    val ipAddresses = accessLogsRDD
      .map(item => (item.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 30)
      .sortBy(-_._2)
      .take(20)

    println(s"IP Address ${ipAddresses.mkString(", ")}")
    /**
      * 需求四：Endpoint
      *     The top endpoints requested by count.
      */
    val topEndpoints: Array[(String, Int)] = accessLogsRDD
      .map(item => (item.endpoint, 1))
      .reduceByKey(_ + _)
      .top(5)(CountUtils.MyOrdering)

    println(s"Endpoint ${topEndpoints.mkString(", ")}")
    // 释放缓存的内容
    accessLogsRDD.unpersist()
    // 为了WEB UI监控, 线程休眠
    Thread.sleep(10000000)

    // 关闭资源
    sc.stop()
  }
}

object CountUtils {


  object MyOrdering extends scala.math.Ordering[(String, Int)] {
    /**
      * 比较两个值的大小
      * @param x
      * @param y
      * @return
      */
    override def compare(x: (String, Int), y: (String, Int)): Int = x._2.compare(y._2)
  }

}

