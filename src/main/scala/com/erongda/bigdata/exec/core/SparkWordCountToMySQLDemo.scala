package com.erongda.bigdata.exec.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于SparkCore实现从HDFS读取数据，进行词频统计WordCount，并且将数据写入到MySQL数据库中
  */
object SparkWordCountToMySQLDemo {

  /**
    * Scala 语言中程序的入口
    * @param args
    *             程序运行传递的参数
    */
  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val sparkConf = new SparkConf().setAppName("SparkWordCountToMySQLDemo").setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(sparkConf)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

    /**
      * 第一步、读取数据，从HDFS读取数据
      */
    val inputRDD: RDD[String] = sc.textFile("/datas/wordcount.data")

    // 样本数据和条目数
    println(s"count = ${inputRDD.count()}")
    println(s"first = ${inputRDD.first()}")
    /**
      * 第二步、数据的分析（调用RDD中转换函数）
      */
    val wordCountRDD = inputRDD.flatMap(line => {
      line.split("\\s+")
    })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    /**
      * 第三步、数据的保存（调用Action数据，更多的是使用foreach）
      */
    // wordCountRDD.foreach(tuple => println(s"word = ${tuple._1}, count = ${tuple._2}"))
    wordCountRDD.persist(StorageLevel.MEMORY_AND_DISK)
    // TODO: 将数据保存到MySQL数据库中
    wordCountRDD.coalesce(1)
      .foreachPartition(iter => {
        Class.forName("com.mysql.jdbc.Driver")
        val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/spark_db"
        val username = "root"
        val password = "123456"
        var conn: Connection = null
        var ps: PreparedStatement =null
        try{
          conn = DriverManager.getConnection(url, username, password)
          val sql_insert = "insert into wordcount_tb values (?, ?)"
          ps = conn.prepareStatement(sql_insert)
          iter.foreach{
            case(word, count) =>
              ps.setString(1, word)
              ps.setInt(2, count)
              ps.executeUpdate()
          }
        }catch {
          case e: Exception => println("MySQL Exception")
        }finally {
          if(ps != null) ps.close()
          if(conn != null) conn.close()
        }
      })
    // 取消缓存
    wordCountRDD.unpersist()
    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }
}
