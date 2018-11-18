package com.erongda.bigdata.exec.hbase

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 使用SparkCore程序实现分组、排序和TopKey，使用分阶段聚合（分为两个阶段）
  */
object SparkGroupSortToSkewExec {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val sparkConf = new SparkConf().setAppName("SparkGroupSortToSkewExec").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    // TODO: 1. 读取数据   ${ContantUtils.LOCAL_DATA_DIC}/group.data
    val inputRDD = sc.textFile(s"${ContantUtils.LOCAL_DATA_DIC}/group.data")
    /**
      * 每行数据的字段信息：
      * aa 78
      * 包含两个字段，各个字段使用 空格 隔开
      */
    val wordCountRDD = inputRDD.map(line => {
      val splited = line.split("\\s+")
      import java.util.Random
      val random = new Random()
      (random.nextInt(2) + "_" + splited(0), splited(1).toInt)
    })
      .groupByKey()
      .map{case (key, iter) => {
        val word = key.split("_")(1)
        val sortList = iter.toList.sortBy(-_).take(3)
        (word, sortList)
      }}
      .groupByKey()
      .map{case (word, iter) =>
        (word, iter.toList.flatten.sortBy(-_).take(3))
      }

    wordCountRDD.coalesce(1).foreachPartition(_.foreach(println))
    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
