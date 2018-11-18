package com.erongda.bigdata.exec.sql

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * 在SparkSQL中提供读取类似CSV、TSV格式数据，基本使用说明
  */
object SparkCSvSQL2Demo {

  def main(args: Array[String]): Unit = {

    // TODO：构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName("SparkCSvSQL2Demo")
      .master("local[3]")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    // TODO: 读取文本数据  ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-1m/ratings.dat"
    val ratingsDS: Dataset[String] = spark.read
      .textFile(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-1m/ratings.dat")
      .map(item => {item.replace("::", ",")})

    // 自定义schema
    val schema = StructType(
      Array(
        StructField("user_id", IntegerType, nullable = false),
        StructField("movie_id", IntegerType, nullable = false),
        StructField("rating", IntegerType, nullable = false),
        StructField("timestamp", IntegerType, nullable = false)
      )
    )

    val ratingDF: DataFrame = spark.read
      .schema(schema)
      .option("inferSchema", "true")
      .csv(ratingsDS)

    // 输出测试
    ratingDF.printSchema()
    ratingDF.show(5, truncate = false)

    // 关闭资源
    spark.stop()

  }

}
