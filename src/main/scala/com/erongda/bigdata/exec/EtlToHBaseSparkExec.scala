package com.erongda.bigdata.exec

import java.util.zip.CRC32

import com.erongda.bigdata.project.common.EventLogConstants
import com.erongda.bigdata.project.common.EventLogConstants.EventEnum
import com.erongda.bigdata.project.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于Spark框架读取HDFS上日志文件数据，进行ETL操作，最终将数据插入到HBase表中
  *   -1. 为什么选择ETL数据到HBase表中？？？
  *     采集的数据包含很多Event事件类型的数据，不同Event事件类型的数据在字段是不一样的，数据量相当大的
  *   -2. HBase 表的设计？？
  *     -a. 每天的日志数据，ETL到一张表中
  *       本系统，主要针对日志数据进行分析的，基本上每天的数据分析一次，为了分析更快，加载更少的数据
  *     -b. 每次ETL数据的时候，创建一张表
  *       表的名称：event_logs  +  日期（年月日），每次先判断表是否存在，存在先删除，后创建
  *       -i. 创建表的时候，考虑常见预分区，使得加载数据到表中的时候，数据分布在不同的Region中，减少写热点，避免Region Split
  *       -ii. 可以考虑表中的数据压缩，使用snappy或lz4压缩
  *     -c. RowKey设计原则：
  *       - 唯一性（不重复）
  *       - 结合业务考虑
  *         某个EventType数据分析，某个时间段数据分析
  *       RowKey = 服务器时间戳 + CRC32(用户ID、会员ID、事件名称）
  */
object EtlToHBaseSparkExec {

  /**
    * 依据字段信息构建RowKey
    * @param time
    *             服务器时间
    * @param uUID
    *             用户ID
    * @param uMD
    *            用户会员ID
    * @param eventAlias
    *                   时间Event名称
    * @return
    *         字符串
    */
  def createRowKey(time: Long, uUID: String, uMD: String, eventAlias: String): String ={
    // 创建StringBuilder实例对象，用于拼接字符串
    val sBuilder = new StringBuilder()
    sBuilder.append(time + "_")

    // 创建CRC32实例对象，进行字符串编码，将字符串转换为Long类型数字
    val crc32 = new CRC32()
    // 重置
    crc32.reset()
    if(StringUtils.isNotBlank(uUID)){
      crc32.update(Bytes.toBytes(uUID))
    }
    if(StringUtils.isNotBlank(uMD)){
      crc32.update(Bytes.toBytes(uMD))
    }
    crc32.update(Bytes.toBytes(eventAlias))

    sBuilder.append(crc32.getValue % 1000000000L)
    // return
    sBuilder.toString()
  }

  /**
    * 创建HBase表，创建的时候判断表是否存在，存在的话先删除后创建
    * @param processDate
    *                    要处理哪天数据的日期 ，格式为：2015-12-20
    * @param conf
    *             HBase Client要读取的配置信息
    * @return
    *         表的名称
    */
  def createHBaseTable(processDate: String, conf: Configuration): String = {
    // 处理时间格式
    val time = TimeUtil.parseString2Long(processDate)
    val date = TimeUtil.parseLong2String(time, "yyyyMMdd")

    // table name
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + date

    // 创建表，先判断是否存在
    var conn: Connection = null
    var admin: HBaseAdmin = null

    // TODO: 占位符
    try{
      conn = ConnectionFactory.createConnection(conf)
      admin =  conn.getAdmin.asInstanceOf[HBaseAdmin]
      // 先判断表是否存在
      if (admin.tableExists(tableName)){
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
      // 创建表的描述符
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      // 创建表的列簇描述符
      val familyDesc = new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME)





    }catch{
      case e: Exception => println("HBase Exception")
    }
    tableName
  }

  /**
    *  Spark Application应用程序的入口
    * @param args
    *             程序的参数，实际业务中需要传递 处理哪天数据的日期（processDate）
    */
  def main(args: Array[String]): Unit = {
    // TODO: 需要传递一个参数，表明ETL处理的数据是哪一天的
    if(args.length < 1){
      println("Usage: EtlToHBaseSpark process_date")
      System.exit(1)
    }

    /**
      * 1. 创建SparkContext实例对象，读取数据，调度Job执行
      */
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("EtlToHBaseSpark Application")
      // 设置使用Kryo序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 构建SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")

    /**
      * TODO：a. 读取日志数据，从HDFS读取
      */
    val eventLogsRDD: RDD[String] = sc.textFile(s"/datas/${args(0)}/20151220.log", minPartitions = 3)


    /**
      * TODO: b. 解析每条日志数据
      */

    // 存储事件类型EventType
    val eventTypeList = List(
      EventEnum.LAUNCH, EventEnum.PAGEVIEW, EventEnum.EVENT,
      EventEnum.CHARGEREFUND, EventEnum.CHARGEREQUEST, EventEnum.CHARGESUCCESS
    )
    // TODO: 通过广播变量将事件类型的列表广播给各个Executor

    /**
      * TODO：c. 过滤数据
      */
    val eventPutsRDD: RDD[(ImmutableBytesWritable, Put)] = null

    /**
      * TODO： d. 将RDD中数据保存到HBase表中
      */
    // a. 配置信息

    /**
      * 由于ETL每天执行一次（ETL失败，再次执行），对原始的数据进行处理，将每天的数据存储到HBase表中
      *   表的名称：
      *       create 'event_logs20151220', 'info'
      */

    // 设置输出表的名称

    // TODO：调用RDD中saveAsNewAPIHadoopFile函数将数据写入到HBase表中


    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
