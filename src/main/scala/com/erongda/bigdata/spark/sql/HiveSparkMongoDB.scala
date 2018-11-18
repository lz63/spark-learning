package com.erongda.bigdata.spark.sql

import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bson.Document

/**
  *  spark从hive中读取数据写入MongoDB
  */
object HiveSparkMongoDB {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("MongoSparkSQL")
      // 指定MongoDB地址、输入数据库及表名
      .config("spark.mongodb.input.uri", "mongodb://localhost/test.col1")
      // 指定MongoDB地址、输出数据库及表名
      .config("spark.mongodb.output.uri", "mongodb://localhost/test")
      .enableHiveSupport()
      .getOrCreate()

    // 设置日志级别
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("======spark从hive中读取数据写入mongodb分析========")

    /**
      * 读取Hive表中的数据进行分析
      */
    val hiveEmpDF: DataFrame = spark
      .sql(
        """
          |SELECT * FROM db_0624.emp
        """.stripMargin)
    hiveEmpDF.printSchema()
    println(s"Count = ${hiveEmpDF.count()}")
    println(s"First = ${hiveEmpDF.first()}")
    hiveEmpDF.show(5, truncate = false)
    hiveEmpDF.write.option("collection", "spark_hive_emp").mode("overwrite").format("com.mongodb.spark.sql").save()
    // 导包
    import com.mongodb.spark._
    // 读取数据
    // 设置读入的配置文件信息
    val readConfig = ReadConfig(Map("collection" -> "spark_hive_emp", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    // 打印基本信息
    val mongoEmpRDD: MongoRDD[Document] = MongoSpark.load(sc, readConfig)
    println(s"Count = ${mongoEmpRDD.count()}")
    println(s"First = ${mongoEmpRDD.first()}")

    println("======spark从hive中读取数据写入mongodbDSL分析========")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    /**
      * TODO: 采用自定义Schema方式将RDD转换为DataFrame
      */
    // a. RDD[Row]
    val rowMongoEmpRDD = mongoEmpRDD.map(line => {
      println(line)
      Row(line.getInteger("empno"),
        line.getString("ename"),
        line.getString("job"),
        line.getInteger("mgr"),
        line.getString("hiredate"),
        line.getDouble("sal"),
        line.getDouble("comm"),
        line.getInteger("deptno")
      )
    })
    println(s"rowMongoEmpRDDCount = ${rowMongoEmpRDD.count()}")
    println(s"rowMongoEmpRDDFirst = ${rowMongoEmpRDD.first()}")

    // 创建Schema
    val mongoEmpSchema: StructType = StructType(
      Array(
        StructField("empno", IntegerType, nullable = false),
        StructField("ename", StringType, nullable = false),
        StructField("job", StringType, nullable = false),
        StructField("mgr", IntegerType, nullable = true),
        StructField("hiredate", StringType, nullable = false),
        StructField("sal", DoubleType, nullable = false),
        StructField("comm", DoubleType, nullable = true),
        StructField("deptno", IntegerType, nullable = false)
      )
    )

    val mongoEmpDF: DataFrame = spark.createDataFrame(rowMongoEmpRDD, mongoEmpSchema)
    mongoEmpDF.printSchema()
    println(s"mongoEmpDFFisrt = ${mongoEmpDF.first()}")
    println("======spark从hive中读取数据写入mongodb DSL分析========")
    mongoEmpDF
      .select($"deptno", $"sal")
      .groupBy($"deptno")
      .agg(round(avg("sal"), 2).alias("avg_sal"))
      .show(5, truncate = false)

    println("======spark从hive中读取数据写入mongodb SQL分析========")

    mongoEmpDF.createOrReplaceTempView("view_tmp_emp")
    spark
      .sql(
        """
          |SELECT deptno, ROUND(AVG(sal), 2) AS avg_sal FROM view_tmp_emp GROUP BY deptno
        """.stripMargin)
      .show(5, truncate = false)

    // 线程休眠
    Thread.sleep(1000000)
    // 关闭资源
    spark.stop()
  }
}
