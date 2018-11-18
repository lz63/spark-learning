//package com.erongda.bigdata.exec.hbase
//
//import com.erongda.bigdata.project.common.EventLogConstants
//import com.erongda.bigdata.project.common.EventLogConstants.EventEnum
//import com.erongda.bigdata.project.util.TimeUtil
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.{Result, Scan}
//import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil
//import org.apache.hadoop.hbase.util.{Base64, Bytes}
//import org.apache.spark.serializer.KryoSerializer
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by fuyun on 2018/10/7.
//  */
//object  NewInstallUserCountSparkDemo {
//  def main(args: Array[String]): Unit = {
//
//    // TODO: 需要传递一个参数，表明读取数据是哪一天的
//    if (args.length < 1) {
//      println("Usage: NewInstallUserCountSparkDemo process_date")
//    }
//
//    /**
//      * 1. 创建SparkContext实例对象，读取数据，调度Job执行
//      */
//    val sparkConf = new SparkConf()
//        .setAppName("NewInstallUserCountSparkDemo")
//        .setMaster("local[3]")
//        .set("spark.serializer", classOf[KryoSerializer].getName)
//        .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
//
//    // 构建SparkContext
//    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("WARN")
//    /** ============================================================================ */
//    /**
//      * 从HBase表中读取数据，依据需要进行过滤筛选
//      *
//      * 在此业务分析中，只需要 事件Event类型为 launch 类型的数据即可
//      *   字段信息：
//      *     - en  -> 过滤字段， e_l 类型数据
//      *     - s_time： 访问服务器的时间
//      *     - version: 平台的版本
//      *     - pl: platform 平台的类型
//      *     - browserVersion: 浏览器版本
//      *     - browserName: 浏览器名称
//      *     - uuid: 用户ID/访客ID，如果此字段为空，说明属于脏数据，不合格，过滤掉，不进行统计分析
//      */
//    // a. 读取配置信息
//    val conf = HBaseConfiguration.create()
//
//    // 处理时间格式
//    val time = TimeUtil.parseString2Long(args(0))
//    val date  = TimeUtil.parseLong2String(time, "yyyyMMdd")
//    // table name
//    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + date
//
//    // b. 设置从HBase那张表读取数据
//    conf.set(TableInputFormat.INPUT_TABLE, tableName)
//
//    /**
//      * 从HBase表中查询数据如何进行过滤筛选呢？？？？？？
//      */
//    // 创建Scan实例对象，扫描表中的数据
//    val scan = new Scan()
//
//    // i. 设置查询某一列簇
//    val FAMILY_NAME: Array[Byte] = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME
//    scan.addFamily(FAMILY_NAME)
//    // ii. 设置查询的列
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME))
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION))
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))
//    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))
//
//    // iii. 设置一个过滤器，en事件类型的值必须为e_l, 注意一点，先查询此列的值，在进行过滤
//    scan.setFilter(
//      new SingleColumnValueFilter(
//        FAMILY_NAME, //
//        Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), //
//        CompareFilter.CompareOp.EQUAL, //
//        Bytes.toBytes(EventEnum.LAUNCH.alias)
//      )
//    )
//
//    /**
//      * 寻找代码:
//      *     conf.set(TableInputFormat.SCAN, convertScanToString(scan))
//      * 说明：
//      *   -1. TableInputFormat.SCAN 的值
//      *     val SCAN: String = "hbase.mapreduce.scan"
//      *
//      */
//    // 设置Scan扫描器，进行过滤操作
//    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray())
//
//    /**
//      *   def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
//            conf: Configuration = hadoopConfiguration,
//            fClass: Class[F],
//            kClass: Class[K],
//            vClass: Class[V]
//          ): RDD[(K, V)]
//      */
//    // c. 调用SparkContext中newAPIHadoopRDD读取表中的数据
////    val eventLogsRDD = sc.newAPIHadoopRDD(
////      conf, // Configuration
////      classOf[TableInputFormat], //
////      classOf[ImmutableBytesWritable], //
////      classOf[Result]
////    )
//
//    // println(s"Count = ${eventLogsRDD.count()}")
//    /*
//      eventLogsRDD.take(5).foreach{ case (key, result) =>
//        println(s"RowKey = ${Bytes.toString(key.get())}")
//        // 从Result中获取每条数据（列簇、列名和值）
//        for (cell <- result.rawCells()) {
//          // 获取列簇
//          val cf = Bytes.toString(CellUtil.cloneFamily(cell))
//          // 获取列名
//          val column = Bytes.toString(CellUtil.cloneQualifier(cell))
//          // 获取值
//          val value = Bytes.toString(CellUtil.cloneValue(cell))
//          println(s"\t $cf:$column = $value -> ${cell.getTimestamp}")
//        }
//      }
//    */
//
//    // TODO：将从HBase表中读取数据进行转换
//
//          // 获取RowKey
//
//          // 获取所有字段的值
//
//
//          // 以元组形式进行返回
//
//
//      // 过滤数据，当 uuid 和 serverTime 为null的话， 过滤掉，属于不合格的数据
//
//
//    // 对获取的 平台向的数据和浏览器相关的数进行处理，组合
//
//        // 获取当前处理日期 为当月的第几天
//
//
//        // 平台维度信息
//
//
//        // 浏览器维度信息整合
//
//
//        // 最后同样以元组形式返回
//
//
//
//    // timePlatformBrowserNewUserRDD.take(10).foreach(println)
//    // 由于后续针对上述RDD使用多次分析，所以进行缓存
//
//
//
//    /**
//      * TODO: -i. 基本维度分析:  时间维度 + 平台维度
//      */
//
//      // 提取字段信息
//
//      // 聚合统计
//
//
//    println("=============================================")
//
//    /**
//      * TODO: 基本维度 + 浏览器维度
//      */
//
//      // 聚合统计
//
//
//    /**
//      * 企业中针对离线分析来说，通常将分析的结果保存到RDBMS表中；实时分析，将结果存储在Redis中
//      */
//
//      // 对分析结果降低RDD分区数
//
//      // a. Connection -> 运行在Executor上
//      //  获取数据库的连接
//
//      // 创建基本维度结果表
//
//
//      // 插入数据到表格
//
//      // b. Insert
//
//
//        // Really Insert
//
//
//      // c. Close Connection
//
//
//        // 创建基本维度+浏览器维度表格
//
//        // 插入数据到表格
//
//        // b. Insert
//
//          // Really Insert
//
//        // c. Close Connection
//
//
//    // 释放缓存的数据
//
//
//    // 为了开发测试，线程休眠，WEB UI监控查看
//
//    // 关闭资源
//
//  }
//}
