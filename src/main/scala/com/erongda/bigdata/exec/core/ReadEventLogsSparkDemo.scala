package com.erongda.bigdata.exec.core

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer

/**
  * SparkCore从HBase表中读取数据：
  *   表的名称： event_logs20151220
  */
object ReadEventLogsSparkDemo {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val spatkConf = new SparkConf().setAppName("ReadEventLogsSparkDemo").setMaster("local[2]")
    // TODO: 设置使用Kryo方式序列化，默认情况下对 simple types, arrays of simple types, or string type
      .set("spark.serializer", classOf[KryoSerializer].getName)
    // To register your own custom classes with Kryo, use the registerKryoClasses method
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable],classOf[Result]))

    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(spatkConf)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

    // a. 读取配置信息
    val conf = HBaseConfiguration.create()

    // b. 设置从HBase那张表读取数据  "event_logs20151220"
    conf.set(TableInputFormat.INPUT_TABLE,"event_logs20151220")

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

    /**
      *  当使用RDD.take(3).foreach() 报如下错误，ImmutableBytesWritable不能进进行序列化
      *     java.io.NotSerializableException: org.apache.hadoop.hbase.io.ImmutableBytesWritable
      * -i. 原因在于：
      *    RDD.take(3) 将数据从Executor中返回Driver端，需要经过网络传输，所以需要对数据进行序列化操作，然而
      *  ImmutableBytesWritable和Result类型都没有实现Java中序列化接口Serializable，所以出错。
      * -ii. 如何解决问题呢？？
      *     Spark大数据分析计算框架来说，默认情况使用Java Serializable对数据进行序列化，设置其他序列化方式。
      *     http://spark.apache.org/docs/2.2.0/tuning.html#data-serialization
      */
    resultRDD.take(5).foreach{
      case (key, result) =>
        println(s"rowKey = ${Bytes.toString(key.get())}")
        for (cell <- result.rawCells()) {
          val cf = Bytes.toString(CellUtil.cloneFamily(cell))
          val colum = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          println(s"\t$cf:$colum = $value -> ${cell.getTimestamp}")
        }
    }

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
