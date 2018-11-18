package com.erongda.bigdata.exec.core

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用SparkCore程序实现分组、排序和TopKey
  */
object SparkGroupSortDemo {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val sparkConf = new SparkConf().setAppName("SparkGroupSortDemo").setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = new SparkContext(sparkConf)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

    // TODO: 1. 读取数据
    val inputRDD = sc.textFile("file:///E:/IDEAworkspace/spark-learning/datas/group.data")
    println(s"count = ${inputRDD.count()}")
    println(s"first = ${inputRDD.first()}")
    /**
      * 每行数据的字段信息：
      *     aa 78
      *     包含两个字段，各个字段使用 空格 隔开
      */
    val sortTopRDD = inputRDD
      // a. 转换数据类型，变为二元组
      .map(line => {
      val splited = line.split("\\s+")
      (splited(0), splited(1).toInt)
    })
      // b. 按照Key进行分组
      .groupByKey()
      // c. 分组以后，对每个组内的数据进行降序排序，获取前3个值
      .map{case (word, iter) =>
        val counts = iter.toList.sorted.takeRight(3).reverse
        (word, counts)
      }
    // 输出
    sortTopRDD.coalesce(1)
      .foreachPartition(item => {
        item.foreach(println)
    })

    // 为了监控显示界面，线程休眠
    Thread.sleep(10000000L)

    // 关闭资源
    sc.stop()
  }

}
