package com.erongda.bigdata.exec.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/**
  * 使用SparkCore程序实现分组、排序和TopKey, 使用combineByKey进行聚合
  */
object SparkGroupCombinerDemo {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkGroupCombinerDemo")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    // input
    val inputRDD = sc.textFile("file:///E:/IDEAworkspace/spark-learning/datas/group.data")
    // 测试
    println(s"count = ${inputRDD.count()}")
    println(s"first = ${inputRDD.first()}")
    /**
      * def combineByKey[C](
          createCombiner: V => C,
          mergeValue: (C, V) => C,
          mergeCombiners: (C, C) => C,
          numPartitions: Int): RDD[(K, C)]
      */
    val wordCountTop = inputRDD.map(line => {
      val splited = line.split("\\s")
      (splited(0), splited(1).toInt)
    })
      .combineByKey(
        (v: Int) => ListBuffer[Int](v),
        (c: ListBuffer[Int], v: Int) => {
          c += v
          c.sortBy(-_).take(3)
        },
        (c1: ListBuffer[Int], c2: ListBuffer[Int]) => {
          c1 ++= c2
          c1.sortBy(-_).take(3)
        }
      )
    wordCountTop.coalesce(1).foreachPartition(_.foreach(println))
    // sleep
    Thread.sleep(10000000L)
    // stop
    sc.stop()
  }

}
