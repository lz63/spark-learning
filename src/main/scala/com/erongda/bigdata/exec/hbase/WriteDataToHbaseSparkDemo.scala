package com.erongda.bigdata.exec.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by fuyun on 2018/10/10.
  */
object WriteDataToHbaseSparkDemo {


  /**
    * 将RDD的数据保存到HBase表中，使用SparkCore中API完成
    */

    def main(args: Array[String]): Unit = {

      /**
        * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
        */
      val sparkConf = new SparkConf().setAppName("WriteDataToHBaseSparkExec").setMaster("local[2]")

      val sc = SparkContext.getOrCreate(sparkConf)
      sc.setLogLevel("WARN")
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
      val wordCountRDD = sc.parallelize(list)
      /**
        * TableOutputFormat向HBase表中写入数据，要求（Key， Value），所以要转换数据类型：
        *   RDD[(ImmutableBytesWritable, Put)]
        */
      val putsRDD = wordCountRDD.map{ case (word, count) =>
        val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word))
        val put = new Put(rowKey.get())
        put.addColumn(
          Bytes.toBytes("info"),
          Bytes.toBytes("count"),
          Bytes.toBytes(count.toString)
        )
        (rowKey, put)
      }

      // TODO: 读取配置信息
      val conf = HBaseConfiguration.create()
      // a. 设置数据保存的表名称
      conf.set(TableOutputFormat.OUTPUT_TABLE, "ht_wordcount")
      // b. 调用RDD#saveAsNewAPIHadoopFile保存数据到HBase表中
      putsRDD.saveAsNewAPIHadoopFile(
        "/datas/spark/hbase/htwordcount" + System.currentTimeMillis(),
        classOf[ImmutableBytesWritable],
        classOf[Put],
        classOf[TableOutputFormat[ImmutableBytesWritable]],
        conf
      )

      // 为了监控显示界面，线程休眠
      Thread.sleep(1000000)

      // 关闭资源
      sc.stop()
    }

}
