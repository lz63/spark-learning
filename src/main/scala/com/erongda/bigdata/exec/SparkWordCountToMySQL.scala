package com.erongda.bigdata.exec

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark 开发大数据框架经典案例：词频统计WordCount
  */
object SparkWordCountToMySQL {

  /**
    * SCALA 程序入口，启动一个JVM Process
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {


    /**
      * TODO 1. 创建SparkContext实例对象      /datas/wordcount.data
      */

    /**
      * TODO 2. 读取HDFS上数据
      */

    /**
      * TODO 3. 针对数据进行 词频统计WordCount：各个单词之间使用空格分割
      */


    /**
      * TODO 4. 将 wordCountRDD 结果保存至 MySQL表中
      */

    // 为了开发测试，对每个Application运行做监控，所以当前线程休眠
    Thread.sleep(10000000)

    // 关闭资源

  }

}

