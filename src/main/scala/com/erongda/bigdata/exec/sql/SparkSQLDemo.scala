package com.erongda.bigdata.exec.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 演示SparkSQL读取各种数据源的数据，进行分析
  */
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {

    // TODO：1. 构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName("SparkSQLDemo")
      .master("local[3]")
      .getOrCreate()

    // From implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // TODO: 获取SparkContext实例对象
    val sc = spark.sparkContext

    // 设置日志级别
    sc.setLogLevel("WARN")

    /**
      * TODO: 读取存储在HDFS上parquet格式数据
      */
    val userDF: DataFrame = spark.read
      .parquet("/datas/resources/users.parquet")

    // 显示样本数
    userDF.printSchema()
    userDF.show(5, truncate = false)

    println("===========================================")

    // TODO: 对于SparkSQL来说，默认情况下，load加载的数据格式为列式存储的parquet格式文件
    val user1DF = spark.read.load("/datas/resources/users.parquet")
    user1DF.printSchema()
    user1DF.show(5, truncate = false)
    println("================= 从MySQL数据库表中读取数据 ==========================")


    /**
      * 从MySQL数据库中读取数据：销售订单表数据 so
      *     def jdbc(url: String, table: String, properties: Properties): DataFrame
      */
    // 连接数据库URL
    val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/"
    val tableName = "order_db.so"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")

    val soTableDF: DataFrame = spark.read.jdbc(url, tableName, props)
    // 样本数据和Schema信息
    soTableDF.printSchema()
    soTableDF.show(5, truncate = false)
    // 增加Where条件
     val predicates: Array[String] = Array("order_amt >= 100")
     val soDF: DataFrame = spark.read.jdbc(url, "order_db.so", predicates, props)
    // 样本数据和Schema信息
    soDF.printSchema()
    soDF.show(5, truncate = false)
    // 样本数据和Schema信息

    println("================= 对封装在DataFrame中数据分析 ==========================")

    /**
      * SparkSQL中分析数据：
      *   -1. SQL 分析： 最原始提供，类似HiveQL
      *   -2. DSL 分析：调用DataFrame/Dataset API
      *
      *  Hive中包含很多自带函数，SparkSQL同样支持
      *     org.apache.spark.sql.functions
      */
    // TODO: DSL 分析，调用API
    import org.apache.spark.sql.functions._
    soDF
      .select($"date", $"order_id", $"user_id", $"order_amt")
      .agg(sum($"order_amt"))
      .withColumnRenamed("sum(order_amt)", "sum_amt")
      .selectExpr("round(sum_amt, 2) as sum_amt")
      .show(5, truncate = false)

    println("---------------------------------------------------")

    // TODO：使用SQL分析
    // a. 将DataFrame注册为一张临时视图（相当于数据库中视图），视图中数据仅仅只读
    soDF.createOrReplaceTempView("view_temp_so")
    // b. 编写SQL分析


    /**
      * 使用DSL分析并保存结果到MySQL表中
      */



    // TODO: 将结果保存到MySQL数据库中




    // 线程休眠
    Thread.sleep(100000)

    // 关闭资源
    spark.stop()
  }

}
