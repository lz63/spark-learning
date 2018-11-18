package com.erongda.bigdata.exec.logs.analystics.etl

import java.util
import java.util.zip.CRC32

import com.erongda.bigdata.project.common.EventLogConstants
import com.erongda.bigdata.project.common.EventLogConstants.EventEnum
import com.erongda.bigdata.project.util.{LogParser, TimeUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.broadcast.Broadcast
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
  *       -i. 创建表的时候，考虑创建预分区，使得加载数据到表中的时候，数据分布在不同的Region中，减少写热点，避免Region Split
  *       -ii. 可以考虑表中的数据压缩，使用snappy或lz4压缩
  *     -c. RowKey设计原则：
  *       - 唯一性（不重复）
  *       - 结合业务考虑
  *         某个EventType数据分析，某个时间段数据分析
  *       RowKey = 服务器时间戳 + CRC32(用户ID、会员ID、事件名称）
  */
object EtlToHBaseSpark {

  /**
    * 依据字段信息构建RowKey
    * @param time
    *             服务器时间
    * @param uUID
    *             用户ID
    * @param uMD
    *            用户会员ID
    * @param eventAlias
    *                   事件Event名称
    * @return
    *         字符串
    */
  def createRowKey(time: Long, uUID: String, uMD: String, eventAlias: String): String ={
    // 创建StringBuilder实例对象，用于拼接字符串
//    val sBuilder = new StringBuilder()
//    sBuilder.append(time + "_")
    val sBuffer = new StringBuffer()
    sBuffer.append(time + "_")

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

    sBuffer.append(crc32.getValue % 1000000000L)
    // return
    sBuffer.toString()
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

    try{
      // 获取连接
      conn = ConnectionFactory.createConnection(conf)
      // 获取HBaseAdmin实例对象
      admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

      // 判断表
      if (admin.tableExists(tableName)) {
        // 先禁用再删除
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }

      // 创建表的描述符
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      // 创建表的列簇描述符
      val familyDesc = new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME)

      /**
        * 针对列簇设置属性
        */
      // 设置数据压缩
      familyDesc.setCompressionType(Compression.Algorithm.SNAPPY)
      // 设置读取数据不缓存
      familyDesc.setBlockCacheEnabled(false)
      // 向表中添加列簇
      desc.addFamily(familyDesc)

      // 设置表的预分区 ，针对整个表来说的，不是针对某个列簇来说的
      // Unit createTable(desc: HTableDescriptor, splitKeys: Array[Array[Byte]])
      admin.createTable(desc, //
        Array(
          Bytes.toBytes("1450570894832"), Bytes.toBytes("1450571184358"),
          Bytes.toBytes("1450571523849"), Bytes.toBytes("1450570479953")
        )
      )
    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != admin) admin.close()
      if (null != conn) conn.close()
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

    // println(s"Count = ${eventLogsRDD.count()}")
    // println(eventLogsRDD.first())


    /**
      * TODO: b. 解析每条日志数据
      */
    val parseEventLogsRDD: RDD[(String, util.Map[String, String])] = eventLogsRDD
      // 通过解析工具类解析每条数据
      .map(line => {
        // 调用工具类进行解析得到Map集合
        val logInfo: util.Map[String, String] = new LogParser().handleLogParser(line)
        // 获取事件的类型
        val eventAlias = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME)
        // 以二元组的形式返回
        (eventAlias, logInfo)
      })
    // println(parseEventLogsRDD.first())

    // 存储事件类型EventType
    val eventTypeList = List(
      EventEnum.LAUNCH, EventEnum.PAGEVIEW, EventEnum.EVENT,
      EventEnum.CHARGEREFUND, EventEnum.CHARGEREQUEST, EventEnum.CHARGESUCCESS
    )
    // TODO: 通过广播变量将事件类型的列表广播给各个Executor
    val eventTypeListBroadcast: Broadcast[List[EventEnum]] = sc.broadcast(eventTypeList)

    /**
      * TODO：c. 过滤数据
      */
    val eventPutsRDD: RDD[(ImmutableBytesWritable, Put)] = parseEventLogsRDD
      // 过滤事件类型EventType不存在的数据; 解析Map集合为空
      // TODO: 性能优化点：将集合列表 拷贝到每个Executor中一份数据，而不是每个Task中一份数据
      .filter{ case (eventAlias, logInfo) =>
        //logInfo.size() != 0 && eventTypeList.contains(EventEnum.valueOfAlias(eventAlias))
        logInfo.size() != 0 && eventTypeListBroadcast.value.contains(EventEnum.valueOfAlias(eventAlias))
      }
      // 数据转换，装备RDD数据，将数据保存到HBase表中RDD[(ImmutableByteWritable, Put)]
      .map{ case (eventAlias, logInfo) =>
        // -i. RowKey表的主键
        val rowKey: String = createRowKey(
          TimeUtil.parseNginxServerTime2Long(logInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)), //
          logInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID), //
          logInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID), //
          eventAlias //
        )
        // -ii. 创建Put对象，添加相应列
        val put = new Put(Bytes.toBytes(rowKey))
        // TODO: 注意此处需要将Java中Map集合转换为Scala中Map集合，方能进行操作
        import scala.collection.JavaConverters._
        for((key, value) <- logInfo.asScala){
          put.addColumn(
            EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME, //
            Bytes.toBytes(key), //
            Bytes.toBytes(value) //
          ) //
        }
        // -iii. 返回二元组
        (new ImmutableBytesWritable(put.getRow), put)
      }

    /**
      * TODO： d. 将RDD中数据保存到HBase表中
      */
    // a. 配置信息
    val conf: Configuration = HBaseConfiguration.create()

    /**
      * 由于ETL每天执行一次（ETL失败，再次执行），对原始的数据进行处理，将每天的数据存储到HBase表中
      *   表的名称：
      *       create 'event_logs20151220', 'info'
      */
      val tableName = createHBaseTable(args(0), conf)

    // 设置输出表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // TODO：调用RDD中saveAsNewAPIHadoopFile函数将数据写入到HBase表中
    eventPutsRDD.saveAsNewAPIHadoopFile(
      "/datas/spark/hbase/etl" + System.currentTimeMillis(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )
    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
