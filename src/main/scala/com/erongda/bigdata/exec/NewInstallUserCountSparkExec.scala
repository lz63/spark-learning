package com.erongda.bigdata.exec

import com.erongda.bigdata.project.common.EventLogConstants
import com.erongda.bigdata.project.common.EventLogConstants.EventEnum
import com.erongda.bigdata.project.util.TimeUtil
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueExcludeFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于SparkCore实现，从HBase表中读取数据，统计新增用户，按照不同维度进行分析
  */
object NewInstallUserCountSparkExec {

  def main(args: Array[String]): Unit = {
    // TODO: 需要传递一个参数，表明读取数据是哪一天的
    if(args.length < 1){
      println("Usage: NewInstallUserCountSpark process_date")
      System.exit(1)
    }

    /**
      * 1. 创建SparkContext实例对象，读取数据，调度Job执行
      */
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("NewInstallUserCountSpark Application")
      // 设置使用Kryo序列化
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
    // 构建SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")

    /** ============================================================================ */
    /**
      * 从HBase表中读取数据，依据需要进行过滤筛选
      *
      * 在此业务分析中，只需要 事件Event类型为 launch 类型的数据即可
      *   字段信息：
      *     - en  -> 过滤字段， e_l 类型数据
      *     - s_time： 访问服务器的时间
      *     - version: 平台的版本
      *     - pl: platform 平台的类型
      *     - browserVersion: 浏览器版本
      *     - browserName: 浏览器名称
      *     - uuid: 用户ID/访客ID，如果此字段为空，说明属于脏数据，不合格，过滤掉，不进行统计分析
      */
    // a. 读取配置信息
    val conf = HBaseConfiguration.create()
    // 处理时间格式
    val time = TimeUtil.parseString2Long(args(0))
    val date = TimeUtil.parseLong2String(time, "yyyyMMdd")
    // table name
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + date

    // b. 设置从HBase那张表读取数据
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    /**
      * 从HBase表中查询数据如何进行过滤筛选呢？？？？？？
      */
    // 创建Scan实例对象，扫描表中的数据
    val scan: Scan = new Scan()

    // i. 设置查询某一列簇
    val FAMILY_NAME: Array[Byte] = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME
    scan.addFamily(FAMILY_NAME)
    // ii. 设置查询的列
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))

    // iii. 设置一个过滤器，en事件类型的值必须为e_l, 注意一点，先查询此列的值，在进行过滤
    scan.setFilter(
      new SingleColumnValueExcludeFilter(
        FAMILY_NAME, // cf
        Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),  // column
        CompareFilter.CompareOp.EQUAL,
        Bytes.toBytes(EventEnum.LAUNCH.alias)  // value
      )
    )
    // 设置Scan扫描器，进行过滤操作
    conf.set(
      TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )

    // c. 调用SparkContext中newAPIHadoopRDD读取表中的数据, 获取前5条数据，解析每列值
    val eventLogsRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    eventLogsRDD.take(5).foreach{ case (key, result) =>
      println(s"RowKey = ${Bytes.toString(key.get())}")
      // 从Result中获取每条数据（列簇、列名和值）
      for (cell <- result.rawCells()) {
        // 获取列簇
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        // 获取列名
        val column = Bytes.toString(CellUtil.cloneQualifier(cell))
        // 获取值
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        println(s"\t $cf:$column = $value -> ${cell.getTimestamp}")
      }
    }

    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(1000000)
    // 关闭资源
    sc.stop()
  }

}
