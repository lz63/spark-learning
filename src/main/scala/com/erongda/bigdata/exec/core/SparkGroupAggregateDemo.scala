package com.erongda.bigdata.exec.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/**
  * 使用SparkCore程序实现分组、排序和TopKey, 使用aggregateByKey进行聚合
  */
object SparkGroupAggregateDemo {

  def main(args: Array[String]): Unit = {

    // conf
    val sparkConf = new SparkConf().setAppName("SparkGroupAggregateDemo").setMaster("local[2]")
    // context
    val sc = SparkContext.getOrCreate(sparkConf)
    // 设置日志级别
    sc.setLogLevel("WARN")
    // input
    val inputRDD = sc.textFile("file:///E:/IDEAworkspace/spark-learning/datas/group.data")
    println(s"count = ${inputRDD.count()}")
    println(s"first = ${inputRDD.first()}")

    val wordCountRDD = inputRDD.map(line => {
      val splited = line.split("\\s+")
      (splited(0), splited(1).toInt)
    })
      // def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,combOp: (U, U) => U
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
    // output
    wordCountRDD.coalesce(1).foreachPartition(_.foreach(println))
    // sleep
    Thread.sleep(10000000L)
    // stop
    sc.stop()
  }

}
