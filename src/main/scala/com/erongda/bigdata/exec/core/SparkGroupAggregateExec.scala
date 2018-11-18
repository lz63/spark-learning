package com.erongda.bigdata.exec.core

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用SparkCore程序实现分组、排序和TopKey, 使用aggregateByKey进行聚合
  */
object SparkGroupAggregateExec {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val sparkConf = new SparkConf().setAppName("SparkGroupAggregateExec").setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(sparkConf)
    // 设置日志级别
    sc.setLogLevel("WARN")
    // TODO: 1. 读取数据  ${ContantUtils.LOCAL_DATA_DIC}/group.data
    val inputRDD = sc.textFile(s"${ContantUtils.LOCAL_DATA_DIC}/group.data")
    /**
      * 每行数据的字段信息：
      *     aa 78
      *     包含两个字段，各个字段使用 空格 隔开
      */
    val wordCountTop = inputRDD.map(line => {
      val splited = line.split("\\s+")
      (splited(0), splited(1).toInt)
    })
      .aggregateByKey(ListBuffer[Int]())(
        (u, v) => {
          u += v
          u.sortBy(-_).take(3)
        },
        (u1, u2) => {
          u1 ++= u2
          u1.sortBy(-_).take(3)
        }
      )

    wordCountTop.coalesce(1).foreachPartition(_.foreach(println))
    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
