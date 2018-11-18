package com.erongda.bigdata.exec.hbase

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkCore从HBase表中读取数据：
  *   表的名称： ns1:sale_orders
  */
object ReadSaleOrderSparkExecDemo {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    val sparkConf = new SparkConf().setAppName("ReadSaleOrderSparkExecDemo").setMaster("local[2]")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable],classOf[Result]))
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    // a. 读取配置信息
    val conf = HBaseConfiguration.create()
    // b. 设置从HBase那张表读取数据
    conf.set(TableInputFormat.INPUT_TABLE, "ns1:sale_orders")
    // c. 调用SparkContext中newAPIHadoopRDD读取表中的数据
    val resultRDD = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    // 测试获取的数据
    resultRDD.take(5).foreach{ case (rowKey, result) =>
      println(s"rowKey = ${rowKey}")
      for(cell <- result.rawCells()) {
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        val column = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        println(s"\t $cf:$column = $value -> ${cell.getTimestamp}")
      }
    }
    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
