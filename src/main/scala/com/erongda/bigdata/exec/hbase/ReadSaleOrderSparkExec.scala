package com.erongda.bigdata.exec.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkCore从HBase表中读取数据：
  *   表的名称： ns1:sale_orders
  */
object ReadSaleOrderSparkExec {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    val sparkConf = new SparkConf().setAppName("ReadSaleOrderSparkExec").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConf)

    // a. 读取配置信息
    val conf =  HBaseConfiguration.create()

    // b. 设置从HBase那张表读取数据
    conf.set(TableInputFormat.INPUT_TABLE,"ns1:sale_orders")

    /**
      *   def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
            conf: Configuration = hadoopConfiguration,
            fClass: Class[F],
            kClass: Class[K],
            vClass: Class[V]
          ): RDD[(K, V)]
      */
    // c. 调用SparkContext中newAPIHadoopRDD读取表中的数据
    val resultRDD = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    // 测试获取的数据
    println(s"count = ${resultRDD.count()}")
    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000L)
    // 关闭资源
    sc.stop()

  }

}
