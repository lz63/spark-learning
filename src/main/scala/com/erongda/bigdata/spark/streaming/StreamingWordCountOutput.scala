package com.erongda.bigdata.spark.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * 从Socket中读物数据，分析处理，将结果打印到 控制台上， 实时接收数据，统计每批次数据中词频WordCount
  *     并将结果保存到MySQL和HBASE中
  */
object StreamingWordCountOutput {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("StreamingWordCount")
      // 序列化
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Put]))
    // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 设置日志级别
    ssc.sparkContext.setLogLevel("WARN")

    /**
      * TODO: 1. 从数据源实时接收流式数据
      *     一直接收数据
      *       def socketTextStream(
                  hostname: String,
                  port: Int,
                  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                ): ReceiverInputDStream[String]
      */
    val inputDStream: DStream[String] = ssc.socketTextStream("bigdata-training01.erongda.com", 9999)

    /**
      * TODO：2. 调用DStream中API对数据分析处理
      *     每批次处理数据: 针对每批次数据进行词频统计（此处每次统计5秒内的数据）
      */
    // a. 分割单词
    val wordDStream = inputDStream.flatMap(line => line.split("\\s+"))
    // b. 转换为二元组
    val tupleDStream = wordDStream.map(word => (word, 1))
    // c. 按照Key聚合统计
    val wordCountDStream = tupleDStream.reduceByKey((a, b) => a + b)
    /**
      * TODO：3. 将每批次处理的结果进行输出
      *     每批次的结果输出
      */
    // wordCountDStream.print(10)
    /*wordCountDStream.foreachRDD(rdd => {
      // TODO：判断结果RDD是否有数据，没有的话， 不进行操作输出；当且仅当有数据的时候进行Output操作
      if(!rdd.isEmpty()){
        println("============================================")
        // TODO: RDD输出至关重要，往往先降低RDD分区数，在对每个分区数据进行输出操作
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
    })*/
    /**
      * def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
      */
    wordCountDStream.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      println("-----------------------------------------")
      val batchTime = time.milliseconds
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      val batchDateTime = sdf.format(new Date(batchTime))
      println(s"Batch Time: $batchDateTime")
      println("-----------------------------------------")
      if(!rdd.isEmpty()){
        // TODO: RDD输出至关重要，往往先降低RDD分区数，在对每个分区数据进行输出操作
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
    })

    /**
      * 将结果保存到MySQL数据库中
      */
    wordCountDStream.foreachRDD(rdd => {
      println("-----保存MySQL开始-----")
      if(!rdd.isEmpty()){
        // 将rdd缓存
        rdd.cache()
        // TODO: 在SparkCore中如何将RDD数据保存MySQL数据库表中编程，再此时一模一样
        rdd.coalesce(1).foreachPartition(iter => {
          // 1. 获取数据库连接Connection
          Class.forName("com.mysql.jdbc.Driver")
          val url = "jdbc:mysql://localhost:3306"
          val user = "root"
          val password = "123456"
          var conn: Connection = null
          var ps_create: PreparedStatement = null
          var ps_insert: PreparedStatement = null
          var ps_drop: PreparedStatement = null
          try{
            conn = DriverManager.getConnection(url, user, password)
            // 删除表
            val sql_drop = "drop table spark_db.streaming_tb"
            ps_drop = conn.prepareStatement(sql_drop)
            ps_drop.execute()
            // 再创建表
            val sql_create = "create table spark_db.streaming_tb(id int(10) primary key auto_increment, word varchar(50), count int(10))"
            ps_create = conn.prepareStatement(sql_create)
            ps_create.execute()
            // 将数据插入表中
            val sql_insert = "insert into spark_db.streaming_tb (word, count) values (?, ?)"
            ps_insert = conn.prepareStatement(sql_insert)
            // 2. 针对分区中的数据插入表中（批量插入）
            iter.foreach(item => {
              ps_insert.setString(1, item._1)
              ps_insert.setInt(2, item._2)
              ps_insert.executeUpdate()
            })
          }catch {
            case e: Exception => println("MySQL Exception")
          }finally {
            // 3. 关闭连接
            if(null != ps_drop) ps_drop.close()
            if(null != ps_create) ps_create.close()
            if(null != ps_insert) ps_insert.close()
            if(null != conn) conn.close()
          }
        })
        println("-----保存MySQL结束-----")
        // 释放缓存
        rdd.unpersist(true)
      }
    })

    // 写入HBase
    wordCountDStream.foreachRDD(rdd => {
      println("-----保存HBase开始-----")
      if (!rdd.isEmpty()) {
        //  不为空，将rdd缓存
        rdd.cache()

        val tableName = "streaming_wordcount"
        // 对rdd的每个分区操作
        rdd.coalesce(1).foreachPartition(iter => {
          import org.apache.hadoop.hbase.client.Connection
          // 获取连接
          var conn: Connection = null
          var table: Table = null
          var admin: HBaseAdmin = null
          val conf = HBaseConfiguration.create()
          try{
            // 获取连接
            conn = ConnectionFactory.createConnection(conf)
            // 获取HBaseAdmin实例对象
            admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
            // 先判断表是否存在，不存在则创建
            if (!admin.tableExists(tableName)) {
              // 创建表的描述符
              val desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(tableName)))
              // 创建表的列簇描述符
              val familyDesc = new HColumnDescriptor(Bytes.toBytes("info"))
              // 设置读取数据不缓存
              familyDesc.setBlockCacheEnabled(false)
              // 向表中添加列簇
              desc.addFamily(familyDesc)
              // 创建表
              admin.createTable(desc)
            }
            // 获取HBase Table的句柄
            table = conn.getTable(TableName.valueOf("streaming_wordcount"))
            iter.foreach { case (word, count) =>
              // 获取rowKey
              val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word))
              // 创建Put对象
              val put = new Put(rowKey.get())
              // 增加列
              put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("count"),
                Bytes.toBytes(count.toString)
              )
              println("-----保存HBase结束-----")
              // 保存到表中
              table.put(put)
            }
          }catch {
            case e: Exception => println("HBase Exception")
          }finally {
            if (null != conn) conn.close()
          }
        })
        // 释放内存
        rdd.unpersist()
      }
    })

    // TODO: 4. 启动实时流式应用，开始准备接收数据并进行处理
    ssc.start()
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如认为终止程序，或者程序出现异常，需要等待
    ssc.awaitTermination()

    // 停止StreamingContext
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}


