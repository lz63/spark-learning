package com.erongda.bigdata.exec.sql

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 在SparkSQL中提供读取类似CSV、TSV格式数据，基本使用说明
  */
object SparkCSvSQLDemo {

  def main(args: Array[String]): Unit = {

    // TODO：构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName("SparkCSvSQLDemo")
      .master("local[3]")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    /**
      * 实际企业数据分析中
      *     csv\tsv格式数据，每个文件的第一行（head, 首行），字段的名称（列名）
      */

    // TODO: 读取TSV格式数据  "E:/IDEAworkspace/spark-learning/datas/als/movielens/ml-100k/u.data"
    val mlRating: DataFrame = spark.read
      .option("sep", "\t") // 设置分隔符
      .option("header", "true")  // 是否以首行作为列名称，默认为false
      .option("inferSchema", "true")   // 是否自动推断schema类型
      .csv("file:///E:/IDEAworkspace/spark-learning/datas/als/movielens/ml-100k/u.data")

    println("=======================================================")
    mlRating.printSchema()
    mlRating.show(5, truncate = false)
    // 定义Schema信息
    val schema = StructType(
      Array(
        StructField("user_id", IntegerType, nullable = true),
        StructField("movie_id", IntegerType, nullable = true),
        StructField("rating", IntegerType, nullable = true),
        StructField("timestamp", IntegerType, nullable = true)
      )
    )

    // TODO: 读取TSV格式数据
    val mlRatingDF = spark.read
      .option("sep", "\t")
      .schema(schema)
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/u.data")

    println("==================自定义schema========================")
    mlRatingDF.printSchema()
    mlRatingDF.show(5, truncate = false)

    /**
      * 将电影评分数据保存为CSV格式数据
      */
    mlRatingDF.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .option("sep", ",")
      .option("header", "true")
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/u.data1")

    println("===========保存成功==================")
    // 关闭资源
    spark.stop()

  }

}
