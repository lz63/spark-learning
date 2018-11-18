package com.erongda.bigdata.spark.sql


import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
  * Spark与MongoDB集成读取数据及写入数据
  */
object MongoSparkSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("MongoSparkSQL")
      // 指定MongoDB地址、输入数据库及表名
      .config("spark.mongodb.input.uri", "mongodb://localhost/test.col1")
      // 指定MongoDB地址、输出数据库及表名
      .config("spark.mongodb.output.uri", "mongodb://localhost/test.spark" + System.currentTimeMillis())
      .getOrCreate()

    // 设置日志级别
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("======spark-mongodb读写分析========")

    // 设置配置：链接地址、数据库及表名
    val writeConfig1 = WriteConfig(Map("uri" -> "mongodb://localhost/test", "collection" -> ("spark_document" + System.currentTimeMillis()), "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    // 创建sparkDocuments
    val sparkDocuments: RDD[Document] = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    // 保存到MongoDB表中
    MongoSpark.save(sparkDocuments, writeConfig1)
     // 读取数据
    val mongoInputDF: DataFrame = MongoSpark.load(spark)
    // 打印数据的相关信息
    println(s"Count = ${mongoInputDF.count()}")
    println(s"First = ${mongoInputDF.first()}")
    mongoInputDF.printSchema()
    mongoInputDF.show(5,truncate = false)
    println("======spark-mongodb读写DSL分析========")
    // TODO: DSL 分析，调用API
    // 导包
    import com.mongodb.spark._
    import spark.implicits._
    mongoInputDF
      .select("name","age")
      .filter($"age"  > 15 && $"name".equalTo("小雨"))
      .orderBy($"age".desc)
      .show(5, truncate = false)
    println("======spark-mongodb读写SQL分析========")
    // TODO: SQL分析
    // 先创建临时视图
    import org.apache.spark.sql.functions._
    mongoInputDF.createOrReplaceTempView("view_tmp_mongo")
    spark.sql(
      """
        |SELECT
        |  name, age
        |FROM
        |  view_tmp_mongo
        |WHERE
        |  age > 15 and name = '小雨'
        |ORDER BY
        |  age DESC
      """.stripMargin).show(5, truncate = false)

    println("======spar将数据写入mongodb========")
    // 保存到MongoDB数据库
    mongoInputDF.write.option("collection", "spark").mode("overwrite").format("com.mongodb.spark.sql").save()
    println("======spar将数据成功写入mongodb========")
    // 线程休眠
    Thread.sleep(1000000)
    // 关闭资源
    spark.stop()
  }
}
