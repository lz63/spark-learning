package com.erongda.bigdata.exec.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将RDD的数据保存到HBase表中，使用SparkCore中API完成
  */
object WriteDataToHBaseSparkExec {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // 创建SparkConf实例对象，配置Spark Application信息
    val config: SparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("WriteDataToHBaseSparkExec")
    // 创建SparkContext实例对象
    val sc = SparkContext.getOrCreate(config)

    /**
      * 模拟数据：
      *   将词频统计的结果存储到HBase表中，
      *   设计表：
      *     表的名称：ht_wordcount
      *     RowKey：word
      *     列簇：info
      *     列：count
      */
    // 创建Scala中集合类列表List
    val list = List(("hadoop", 234), ("spark", 3454), ("hive", 343434), ("ml", 8765))
    // 通过并行化集合创建RDD
    val wordcountRDD = sc.parallelize(list)

    /**
      * TableOutputFormat向HBase表中写入数据，要求（Key， Value），所以要转换数据类型：
      *   RDD[(ImmutableBytesWritable, Put)]
      */
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = wordcountRDD.map{
      case (word, count) =>
        // 创建RowKey
        val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word))
        // 构建Put对象
        val put = new Put(rowKey.get())
        // 添加列
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))

        // 返回
        (rowKey, put)
    }

    // TODO: 读取配置信息
    val conf: Configuration = HBaseConfiguration.create()

    // a. 设置数据保存的表名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, "ht_wordcount")

    // b. 设置输出格式OutputFormat
    conf.set("mapreduce.job.outputformat.class",
      "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

    // c. 设置输出路径
    conf.set("mapreduce.output.fileoutputformat.outputdir",
      "/datas/spark/hbase/htwordcount" + System.currentTimeMillis())

    // def saveAsNewAPIHadoopDataset(conf: Configuration): Unit
    putsRDD.saveAsNewAPIHadoopDataset(conf)

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
