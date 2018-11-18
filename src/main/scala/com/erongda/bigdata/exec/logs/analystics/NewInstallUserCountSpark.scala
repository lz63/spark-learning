package com.erongda.bigdata.exec.logs.analystics

import java.util.Calendar

import com.erongda.bigdata.project.common.EventLogConstants
import com.erongda.bigdata.project.common.EventLogConstants.EventEnum
import com.erongda.bigdata.project.etl.ConnectionUtils
import com.erongda.bigdata.project.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RuntimeConfig, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于SparkCore实现，从HBase表中读取数据，统计新增用户，按照不同维度进行分析
  */
object NewInstallUserCountSpark {

  def main(args: Array[String]): Unit = {
    // TODO: 需要传递一个参数，表明读取数据是哪一天的
    if(args.length < 1){
      println("Usage: NewInstallUserCountSpark process_date")
      System.exit(1)
    }

    /**
      * 1. 创建SparkContext实例对象，读取数据，调度Job执行
      */
    /*val spark = SparkSession.builder()
      .appName("NewInstallUserCountSpark")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val sc = spark.sparkContext*/
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("NewInstallUserCountSpark Application")
      // 设置使用Kryo序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))

    // 构建SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", "4")
      .config(sparkConf)
      .getOrCreate()
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
    val scan = new Scan()

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
      new SingleColumnValueFilter(
        FAMILY_NAME, // cf
        Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), // column
        CompareFilter.CompareOp.EQUAL, // compare
        Bytes.toBytes(EventEnum.LAUNCH.alias)
      )
    )

    /**
      * 寻找代码:
      *     conf.set(TableInputFormat.SCAN, convertScanToString(scan))
      * 说明：
      *   -1. TableInputFormat.SCAN 的值
      *     val SCAN: String = "hbase.mapreduce.scan"
      *
      */
    // 设置Scan扫描器，进行过滤操作
    conf.set(
      TableInputFormat.SCAN, //
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) //
    )

    /**
      *   def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
            conf: Configuration = hadoopConfiguration,
            fClass: Class[F],
            kClass: Class[K],
            vClass: Class[V]
          ): RDD[(K, V)]
      */
    // c. 调用SparkContext中newAPIHadoopRDD读取表中的数据
    val eventLogsRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf, // Configuration
      classOf[TableInputFormat], //
      classOf[ImmutableBytesWritable], //
      classOf[Result]
    )

    // println(s"Count = ${eventLogsRDD.count()}")
    /*
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
    */

    // TODO：将从HBase表中读取数据进行转换
    val newUserRDD: RDD[(String, String, String, String, String, String)] = eventLogsRDD
      .map{
        case (key, result) =>
          // 获取RowKey
          val rowKey = Bytes.toString(key.get())
          // 获取所有字段的值
          val uuid = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)))
          val serverTime = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)))
          val platformName = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)))
          val platformVersion = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)))
          val browserName = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)))
          val browserVersion = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)))

          // 以元组形式进行返回
          (uuid, serverTime, platformName, platformVersion, browserName, browserVersion)
      }
      // 过滤数据，当 uuid 和 serverTime 为null的话， 过滤掉，属于不合格的数据
      .filter(tuple => null != tuple._1 && null != tuple._2)

    // 对获取的 平台向的数据和浏览器相关的数进行处理，组合
    val timePlatformBrowserNewUserRDD: RDD[(String, Int, String, String)] = newUserRDD.map{
      case (uuid, serverTime, platformName, platformVersion, browserName, browserVersion) =>
        // 获取当前处理日期 为当月的第几天
        val calendar = Calendar.getInstance()
        calendar.setTimeInMillis(TimeUtil.parseNginxServerTime2Long(serverTime))
        val day = calendar.get(Calendar.DAY_OF_MONTH)

        // 平台维度信息
        var platformDimension: String = ""
        if(StringUtils.isBlank(platformName)){
          platformDimension = "unknown:unknown"
        }else if(StringUtils.isBlank(platformVersion)){
          platformDimension = platformName + ":unknown"
        }else{
          platformDimension = platformName + ":" + platformVersion
        }

        // 浏览器维度信息整合
        var browserDimension: String = ""
        if(StringUtils.isBlank(browserName)){
          browserDimension = "unknown:unknown"
        }else if(StringUtils.isBlank(browserVersion)){
          browserDimension = browserName + ":unknown"
        }else{
          if(0 <= browserVersion.indexOf(".")){
            browserDimension = browserName + ":" + browserVersion.substring(0, browserVersion.indexOf("."))
          }else {
            browserDimension = browserName + ":" + browserVersion
          }
        }

        // 最后同样以元组形式返回
        (uuid, day, platformDimension, browserDimension)
    }

    // timePlatformBrowserNewUserRDD.take(10).foreach(println)
    // 由于后续针对上述RDD使用多次分析，所以进行缓存
//    timePlatformBrowserNewUserRDD.persist(StorageLevel.MEMORY_AND_DISK)

      // 导入SparkSession的隐式转换包
    import spark.implicits._
    // 先创建case类，再将RDD转换为DataFrame
    val timePlatformBrowserNewUserDF: DataFrame = timePlatformBrowserNewUserRDD.map{
      case (uuid, day, platformDimension, browserDimension) =>
        TimePlatformBrowserNewUser(uuid, day, platformDimension, browserDimension)
    }.toDF()
    timePlatformBrowserNewUserDF.persist(StorageLevel.MEMORY_AND_DISK)
    timePlatformBrowserNewUserDF.printSchema()
    // 基本维度分析
    println("=================基本维度分析:DSL实现============================")
    val tpbnDF = timePlatformBrowserNewUserDF
      .select($"day", $"platformDimension")
      .groupBy($"day", $"platformDimension").count()
      .orderBy($"count".desc)
      .show(10, truncate = false)


    println("=================基本维度分析:SQL实现============================")
    timePlatformBrowserNewUserDF.createOrReplaceTempView("view_tmp_tpbn")
    val tpbnSQL = spark.sql(
      """
        |select
        |  day, platformDimension, count(1) as count
        |from
        |  view_tmp_tpbn
        |group by
        |  day, platformDimension
        |order by
        |  count desc
      """.stripMargin).show(10, truncate = false)


    println("=================基本维度 + 浏览器维度分析:DSL实现============================")
    val tpbncDF = timePlatformBrowserNewUserDF
      .select($"day", $"platformDimension", $"browserDimension")
      .groupBy($"day", $"platformDimension", $"browserDimension").count()
      .orderBy($"count".desc)
      .show(10, truncate = false)


    println("=================基本维度 + 浏览器维度分析:SQL实现============================")
    val tpbncSQL = spark.sql(
      """
        |select
        |  day, platformDimension,browserDimension, count(1) as count
        |from
        |  view_tmp_tpbn
        |group by
        |  day, platformDimension, browserDimension
        |order by
        |  count desc
      """.stripMargin).show(10, truncate = false)


    /**
      * TODO: -i. 基本维度分析:  时间维度 + 平台维度
      */
    /*val dayPlatformNewUserCountRDD: RDD[((Int, String), Int)] = timePlatformBrowserNewUserRDD
      // 提取字段信息
      .map(tuple => ((tuple._2, tuple._3), 1))
      // 聚合统计
      .reduceByKey(_ + _)
    dayPlatformNewUserCountRDD.foreachPartition(_.foreach(println))

    println("=============================================")

    /**
      * TODO: 基本维度 + 浏览器维度
      */
    val dayPlatformBrowserUserCountRDD = timePlatformBrowserNewUserRDD
      .map(tuple => ((tuple._2, tuple._3, tuple._4), 1))
      // 聚合统计
      .reduceByKey(_ + _)
    dayPlatformBrowserUserCountRDD.foreachPartition(_.foreach(println))
*/
    /**
      * 企业中针对离线分析来说，通常将分析的结果保存到RDBMS表中；实时分析，将结果存储在Redis中
      */
    /*val dayPlatformTableName = tableName+ "_dayPlatform"
    dayPlatformNewUserCountRDD
      .coalesce(1) // 对分析结果降低RDD分区数
      .foreachPartition(iter => {
        // a. Connection -> 运行在Executor上
        //  获取数据库的连接
        val conn = ConnectionUtils.getConn()
        // 创建基本维度结果表
        val sql_createdayPlatform = s"create table if not exists ${dayPlatformTableName} (id int(11) not null primary key auto_increment, platform int(10), version varchar(50), counts int(11))"
        val ps_createdayPlatform = conn.prepareStatement(sql_createdayPlatform)
        val line = ps_createdayPlatform.executeUpdate()
        println(line)
        // 插入数据到表格
        val sql_insertdayPlatform = s"insert into ${dayPlatformTableName} (platform, version, counts) values (?,?,?)"
        val ps_insertdayPlatform = conn.prepareStatement(sql_insertdayPlatform)
        // b. Insert
        iter.foreach(item => {
          println(s"${item._1._1}  ${item._1._2}  ${item._2}")
          // Really Insert
          ps_insertdayPlatform.setInt(1, item._1._1)
          ps_insertdayPlatform.setString(2, item._1._2)
          ps_insertdayPlatform.setInt(3, item._2)
          ps_insertdayPlatform.executeUpdate()
        })

        // c. Close Connection
        ConnectionUtils.closeConnection(conn)
        })
    val dayPlatformBrowserTableName = tableName+ "_dayPlatformBrowser"
    dayPlatformBrowserUserCountRDD
      .coalesce(1)
      .foreachPartition(iter => {
        val conn = ConnectionUtils.getConn()
        // 创建基本维度+浏览器维度表格
        val sql_createdayPlatformBrowser = s"create table if not exists ${dayPlatformBrowserTableName} (id int(11) not null primary key auto_increment, platform int(10), version varchar(50), browser varchar(50), counts int(11))"
        val ps_createdayPlatformBrowser = conn.prepareStatement(sql_createdayPlatformBrowser)
        ps_createdayPlatformBrowser.executeUpdate()
        // 插入数据到表格
        val sql_insertdayPlatformBrowser = s"insert into ${dayPlatformBrowserTableName} (platform, version, browser, counts) values (?,?,?,?)"
        val ps_insertdayPlatformBrowser = conn.prepareStatement(sql_insertdayPlatformBrowser)
        // b. Insert
        iter.foreach(item => {
          println(s"${item._1._1}  ${item._1._2}  ${item._1._3} ${item._2}")
          // Really Insert
          ps_insertdayPlatformBrowser.setInt(1, item._1._1)
          ps_insertdayPlatformBrowser.setString(2, item._1._2)
          ps_insertdayPlatformBrowser.setString(3,  item._1._3)
          ps_insertdayPlatformBrowser.setInt(4, item._2)
          ps_insertdayPlatformBrowser.executeUpdate()
        })
        // c. Close Connection
        if(conn != null){
          ConnectionUtils.closeConnection(conn)
        }
      })
*/
    // 释放缓存的数据
    timePlatformBrowserNewUserDF.unpersist()

    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(100000)
    // 关闭资源
    sc.stop()
  }


}
