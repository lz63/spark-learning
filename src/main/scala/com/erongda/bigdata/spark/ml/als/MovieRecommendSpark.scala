package com.erongda.bigdata.spark.ml.als

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于DataFrame API实现Spark ML中协同过滤推荐算法ALS进行电影推荐
  */
object MovieRecommendSpark {

  def main(args: Array[String]): Unit = {

    // TODO: 1. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName("MovieRecommendSpark")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    // 导入隐式转换

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 自定义Schame信息
    val mlSchema = StructType(
      Array(
        StructField("userId", IntegerType, nullable = true),
        StructField("movieId", IntegerType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    // TODO: 读取电影评分数据，数据格式为TSV格式
    val rawRatingsDF: DataFrame = spark.read
      // 设置分隔符
      .option("sep", "\t")
      // 设置schema信息
      .schema(mlSchema)
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/u.data")

    rawRatingsDF.printSchema()
    /**
      * root
         |-- userId: integer (nullable = true)
         |-- movieId: integer (nullable = true)
         |-- rating: double (nullable = true)
         |-- timestamp: long (nullable = true)
      */
    rawRatingsDF.show(10, truncate = false)

    // TODO: ALS 算法实例对象
    val als = new ALS() // def this() = this(Identifiable.randomUID("als"))
      // 设置迭代的最大次数
      .setMaxIter(10)
      // 设置特征数
      .setRank(10)
      // 显式评分
      .setImplicitPrefs(false)
      // 设置Block的数目， be partitioned into in order to parallelize computation (默认值: 10).
      .setNumItemBlocks(4).setNumUserBlocks(4)
      // 设置 用户ID:列名、产品ID:列名及评分:列名
      .setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    // TODO: 使用数据训练模型
    val mlAlsModel: ALSModel = als.fit(rawRatingsDF)


    // 用户-特征 因子矩阵
    val userFeaturesDF: DataFrame = mlAlsModel.userFactors
    userFeaturesDF.show(5, truncate = false)
    // 产品-特征 因子矩阵
    val itemFeaturesDF: DataFrame = mlAlsModel.itemFactors
    itemFeaturesDF.show(5, truncate = false)

    // TODO: 为用户推荐10个产品（电影）
    // max number of recommendations for each user
    val userRecs: DataFrame = mlAlsModel.recommendForAllUsers(10)
    userRecs.show(5, truncate = false)
    // 查找 某个用户推荐的10个电影，比如用户:196     将结果保存Redis内存数据库，可以快速查询检索

    // TODO: 为产品（电影）推荐10个用户
    // max number of recommendations for each item
    val movieRecs: DataFrame = mlAlsModel.recommendForAllItems(10)
    movieRecs.show(5, truncate = false)
    // 查找 某个电影推荐的10个用户，比如电影:242

    // TODO: 预测 用户对产品（电影）评分
    val predictRatingsDF: DataFrame = mlAlsModel
      // 设置 用户ID:列名、产品ID:列名
      .setUserCol("userId").setItemCol("movieId")
      .setPredictionCol("predictRating") // 默认列名为 prediction
      .transform(rawRatingsDF)
    predictRatingsDF.show(5, truncate = false)

    // TODO: 模型的评估
    val evaluator = new RegressionEvaluator()
      // 设置评估标准，此处设置为均方根
      .setMetricName("rmse")
      // 设置标签列名为等级
      .setLabelCol("rating")
      // 设置预测列名
      .setPredictionCol("predictRating")
    // 使用上面设置好的评估模型进行评估
    val rmse = evaluator.evaluate(predictRatingsDF)
    println(s"Root-mean-square error = $rmse")

    // TODO: 模型的保存
    mlAlsModel.save(ContantUtils.LOCAL_DATA_DIC + "/als/mlalsModel")

    // TODO: 加载模型
    val loadMlAlsModel: ALSModel = ALSModel.load(ContantUtils.LOCAL_DATA_DIC + "/als/mlalsModel")
    loadMlAlsModel.recommendForAllItems(10).show(5, truncate = false)
    // 线程休眠
    Thread.sleep(100000)

    // 关闭资源
    spark.stop()
  }

}
