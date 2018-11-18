package com.erongda.bigdata.project.TOP10

import java.util.Properties

import com.erongda.bigdata.spark.streaming.ObjectMapperSingleton
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel


/**
  * 美团各区域热门商品Top10统计分析
  */
object ItemTop10 {

  def main(args: Array[String]): Unit = {

    // TODO: 构建SparkSession实例对象
    val spark = SparkSession
      .builder()
      .appName("ItemTop10")
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "3")
      // 告知SparkSession与Hive集成
      .enableHiveSupport()
      .getOrCreate()
    // 导入隐式转换包
    import spark.implicits._

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    /**
      * 读取Hive表中的数据进行分析
      */
    // TODO: 1、读取hive中user_visit_action的分区数据并统计条数展示前10条
    val userVisitActionDF: DataFrame = spark
      .sql(
        s"""
          |SELECT
          |  *
          |FROM
          |  db_top10.user_visit_action
          |WHERE
          |  datestr='20170311'
        """.stripMargin)

    userVisitActionDF.createOrReplaceTempView("view_tmp_user_visit_action")


    // TODO：2、读取MySQL中city_info表中的数据并展示schema、前10条信息、统计条数
    val cityInfoDF: DataFrame = readJdbc("testdb.city_info", spark)
    cityInfoDF.createOrReplaceTempView("view_tmp_city")
    val userVisitActionCityDF = spark.sql(
      """
        |SELECT
        |  u.*,c.city_name, c.province_name, c.area
        |FROM
        |  view_tmp_user_visit_action AS u
        |JOIN
        |  view_tmp_city AS c
        |ON
        |  u.city_id = c.city_id
      """.stripMargin)
//    userVisitActionCityDF.show(10, truncate = false)

    // TODO: 3、缓存
    userVisitActionCityDF.persist(StorageLevel.MEMORY_AND_DISK)
    // 注册为临时视图
    userVisitActionCityDF.createOrReplaceTempView("view_tmp_user_visit_city")
    // 测试
    println("=======================用户行为及城市区域信息表:userVisitActionCityDF==================================")
    userVisitActionCityDF.show(10, truncate = false)
    println(s"userVisitActionCity First = ${userVisitActionCityDF.first()}")

    // TODO: 4、统计各个区域各个商品的点击次数，并注册成为临时视图
    val areaCountDF: DataFrame = spark.sql(
      """
        |SELECT
        |  area, click_product_id,
        |  COUNT(CASE WHEN LENGTH(click_product_id)>0 THEN click_product_id ELSE null END) AS area_product_cnt
        |FROM
        |  view_tmp_user_visit_city
        |GROUP BY
        |  area, click_product_id
        |ORDER BY
        |  area_product_cnt DESC
        |
      """.stripMargin)
    // 注册的为临时表
    areaCountDF.createOrReplaceTempView("view_tmp_area_product_count")
    // 测试
    println("=======================各个区域各个商品的点击次数信息表:areaCountDF==================================")
    areaCountDF.show(10, truncate = false)
    // TODO：5、获取各个区域Top10的点击数据，并注册成为临时视图
    val areaTopTenDF: DataFrame = spark.sql(
      """
        |SELECT
        |  b.area, b.click_product_id, b.area_product_cnt, b.area_product_top
        |FROM
        |  (
        |  SELECT
        |    a.area, a.click_product_id, a.area_product_cnt,
        |    ROW_NUMBER() OVER(PARTITION BY a.area ORDER BY a.area_product_cnt desc) as area_product_top
        |  FROM
        |    view_tmp_area_product_count AS a
        |  ) AS b
        |WHERE b.area_product_top <= 10
      """.stripMargin)
    // 注册为临时视图
    areaTopTenDF.createOrReplaceTempView("view_tmp_areaTopTen")
    // 测试
    println("=======================各个区域Top10的点击数据:areaTopTenDF==================================")
    areaTopTenDF.show( truncate = false)

    // TODO：6、读取hive中product_info的分区数据并统计条数展示前10条
    val productInfoDF: DataFrame = spark.sql(
      """
        |SELECT
        |  *
        |FROM
        |  db_top10.product_info
        |WHERE
        |  datestr='20170311'
      """.stripMargin)
    productInfoDF.createOrReplaceTempView("view_tmp_product_info")
    // 测试
    println("=======================商品信息表:productInfoDF==================================")
    productInfoDF.show(10, truncate = false)

    // TODO：7、将Top10的结果数据和商品表进行关联，得到具体的商品信息，并注册成为临时视
    val areaTopTenProduct: DataFrame = areaTopTenDF.join(productInfoDF,$"product_id" === $"click_product_id")
    areaTopTenProduct.createOrReplaceTempView("view_tmp_area_topten_product")
    // 测试
    println("=======================Top10的结果数据和商品表进行关联:areaTopTenProduct==================================")
    areaTopTenProduct.show(10, truncate = false)
    // TODO: 8、自定义udf解析json格式的字段extend_info
    // sparkSession.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
    spark.udf.register("parse_extend_info",
      (extendInfo: String) => {
        val productTypeId: Int = analysisObjectMapper(extendInfo).productTypeId
        productTypeId
      }
    )

    // TODO: 9、持久化保存在临时表中的结果数据到关系型数据库中
    val areaTopTenProductResult: DataFrame = spark.sql(
      """
        |SELECT
        |  a.area AS area,
        |  (CASE WHEN
        |    a.area IN ('华东','华南','华北')
        |  THEN
        |    'A'
        |  WHEN
        |    a.area=='东北'
        |  THEN
        |    'B'
        |  WHEN
        |    a.area IN ('华中','西南')
        |  THEN
        |    'C'
        |  WHEN
        |    a.area=='西北'
        |  THEN
        |    'D'
        |  ELSE
        |    'E'
        |  END) AS area_level,
        |  b.area_product_cnt as click_count,
        |  CONCAT(a.city_id, ":", a.city_name) AS city_infos,
        |  b.product_id,
        |  b.product_name,
        |  (CASE WHEN
        |    parse_extend_info(b.extend_info)==0
        |  THEN
        |    '自营'
        |  ELSE
        |    '第三方'
        |  END ) AS product_type
        |FROM
        |  view_tmp_user_visit_city a
        |JOIN
        |  view_tmp_area_topten_product b
        |ON
        |  a.click_product_id = b.product_id
        |
      """.stripMargin)

    // 测试
    println("=======================Top10的结果数据和商品表进行关联的结果表:areaTopTenProductResult==================================")
    areaTopTenProductResult.show(10, truncate = false)

    // 缓存
    areaTopTenProductResult.persist(StorageLevel.MEMORY_AND_DISK)

    // 保存到MySQL数据库中
    savaTableMysql("testdb.tb_area_top10_product", areaTopTenProductResult, spark)
    println("=======================Top10的结果数据和商品表进行关联的结果表保存成功==================================")

    // TODO: 10、释放缓存
    userVisitActionCityDF.unpersist()
    areaTopTenProductResult.unpersist()
    // 线程休眠
    Thread.sleep(1000000L)

    // TODO: 11、关闭资源
    spark.stop()
  }

  /***
    * 连接MySQL数据库读取输入表名的数据
    * @param tableName
    *                  想要读取的表名
    * @param spark
    *              SparkSession
    * @return
    *         表中的数据
    */
  def readJdbc(tableName: String,spark: SparkSession): DataFrame ={
    val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")
    spark.read.jdbc(url, tableName, props)
  }

  /***
    * 将输入表名的表保存到MySQL数据库中
    * @param tableName
    *                  需要保存的表名
    * @param df
    *           需要保存数据的DataFrame
    * @param spark
    *              SparkSession
    */
  def savaTableMysql(tableName: String, df: DataFrame, spark: SparkSession) {
    val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/?characterEncoding=UTF-8"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")
    df.write.mode(SaveMode.Overwrite).jdbc(url, tableName, props)
  }

  /***
    * 用于解析json格式的字段extend_info
    * @param jsonString
    *                   json格式的字段
    * @return
    *         product
    */
  def analysisObjectMapper(jsonString:String): productType ={
    val mapper = ObjectMapperSingleton.getInstance()
    // 如果Jackson反序列化错误：com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field的解决方法一：
    // mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    // import com.fasterxml.jackson.databind.DeserializationFeature
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    // 解析JSON
    val product: productType = mapper.readValue(jsonString, classOf[productType])
    product
  }

}
// 如果Jackson反序列化错误：com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field的解决方法二：
// @JsonIgnoreProperties(ignoreUnknown = true)
// import com.fasterxml.jackson.annotation.JsonIgnoreProperties
@JsonIgnoreProperties(ignoreUnknown = true)
case  class productType(productType: String, productTypeId: Int)

