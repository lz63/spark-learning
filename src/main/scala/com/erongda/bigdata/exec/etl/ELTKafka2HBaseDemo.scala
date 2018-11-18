package com.erongda.bigdata.exec.etl

import com.alibaba.fastjson.JSON
import com.erongda.bigdata.utils.ConstantUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 从Kafka中读取订单数据，根据订单类型保存到HBase对应的表中
  */
object ELTKafka2HBaseDemo {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，设置应用相关配置
    val sparkConf = new SparkConf()
      .setAppName("ELTKafka2HBaseDemo")
      .setMaster("local[2]")
    // 创建StreamingContext实例对象，传入SparkConf并设置批次时间
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 采用Direct方式从Kafka Topic中pull拉取订单数据
    /**
      * ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
      */
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> ConstantUtils.METADATA_BROKER_LIST,
      "auto.offset.reset" -> "smallest"
    )
    // 指定Topic
    val topics: Set[String] = Set(ConstantUtils.ORDER_TOPIC)

    val inputDS: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, //
      kafkaParams,  //
      topics
    )

    // 将订单数据转换封装
    inputDS.map(_._2).foreachRDD(rdd => {
      // 1. 解析JSON格式字符串，获取 orderType类型
      val orderRDD: RDD[(String, String)] = rdd.mapPartitions(iter => {
        iter.map(item => {
          // 解析获取orderType
          val orderType = JSON.parseObject(item).getString("orderType")
          // 返回
          (orderType, item)
        })
      })
      // 2. 分区，自定义分区器
      val partitionRDD: RDD[(String, String)] = orderRDD.partitionBy(MyPartitioner)

      // 3. 将分区数据写入HBase表
      partitionRDD.foreachPartition(iter => {
        // 通过TaskContext获取当前处理分区数据的分区ID
        TaskContext.getPartitionId() match {
          case 0 => insertIntoHBase("htb_alipay", iter)
          case 1 => insertIntoHBase("htb_weixin", iter)
          case 2 => insertIntoHBase("htb_card", iter)
          case _ => insertIntoHBase("htb_other", iter)
        }
      })
    })

    // 启动实时流应用
    ssc.start()
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如认为终止程序，或者程序出现异常，需要等待
    ssc.awaitTermination()
    // 停止StreamingContext
    ssc.stop(true, true)
  }

  /***
    *  将JSON格式数据插入到HBase表中，其中表的ROWKEY为orderType（解析JSON格式获取）
    * @param tableName
    *                  表的名称
    * @param iter
    *             迭代器，二元组中Value为存储的数据，在HBase表中列名为info:value
    */
  def insertIntoHBase(tableName: String, iter: Iterator[(String, String)]): Unit ={

    // 获取HBaseConf
    var conf: Configuration = null
    // 获取连接
    var conn: Connection = null
    try {
      conf = HBaseConfiguration.create()
      conn = ConnectionFactory.createConnection(conf)
      // 获取句柄
      val table: HTable = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

      // 迭代插入
      import java.util
      val puts = new util.ArrayList[Put]()
      iter.foreach{case (orderType, jsonValue) =>
        // 获取JSON，获取RowKey
        val rowKey = Bytes.toBytes(JSON.parseObject(jsonValue).getString("orderType"))
        // 获取put
        val put = new Put(rowKey)
        // 添加列
        put.addColumn(
          Bytes.toBytes("info"),
          Bytes.toBytes("value"),
          Bytes.toBytes(jsonValue)
        )
        // 将put加入到List中
        puts.add(put)
      }
      // c. 批量插入数据到HBase表中
      table.put(puts)

    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      // 关闭连接
      if (null != conn) conn.close()
    }




  }
}

object MyPartitioner extends Partitioner{
  // 设置分区数为4个
  override def numPartitions: Int = 4

  // 按照订单类型得到不同分区id
  override def getPartition(key: Any): Int = {
    // 获取key中orderType
    key.asInstanceOf[String] match {
      // "alipay", "weixin", "card", "other"
      case "alipay" => 0
      case "weixin" => 1
      case "card" => 2
      case _ => 3
    }
  }
}

