package com.erongda.bigdata.spark.test

import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fuyun on 2018/10/8.
  */
case class  ApacheAccessLog(
                           ipAdress: String,
                           clientIdentd: String,
                           userId: String,
                           dateTime: String,  // 时区
                           method: String,  // 访问方式
                           endpoint: String,  // 最后访问地址
                           protocol: String,  // 协议
                           responseCode: Int,  // 响应码
                           contentSize: Long  //内容大小
                           ){
  val ip = ipAdress
  val client = clientIdentd
  val uId = userId
  var dTime = dateTime.replace("[","")
  val method1= method.replace("\"","")
  val point = endpoint
  val pr = protocol.replace("\"","")
  val res = responseCode
  val cont = contentSize


}

object test{
  def main(args: Array[String]): Unit = {
    //
    val sparkConf = new SparkConf().setAppName("ApacheAccessLog").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    // input
    // val inputRDD = sc.textFile("file:///E:/IDEAworkspace/spark-learning/datas/access_log")
    val inputRDD = sc.textFile("hdfs:///opt/datas/access_log")
    // 测试
    println(s"count = ${inputRDD.count()}")
    println(s"first = ${inputRDD.first()}")
    val splitedRDD = inputRDD.map(line => {
      val splitedList = line.split("\\s+")
      // 合法判断
      if(splitedList.length == 10){
        val ipAdress = splitedList(0)
        val clientIdentd = splitedList(1)
        val userId = splitedList(2)
        // val dT = splitedList(3).replace("[","")
        val dateTime = splitedList(3).replace("[","")
//        import java.text.SimpleDateFormat;
//        val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH)
        try{
          // val dateTime = sdf.parse(dT).toString
          val method = splitedList(5).replace("\"","")
          val endpoint = splitedList(6)
          val protocol = splitedList(7).replace("\"","")
          if (splitedList(8) != "-" && splitedList(9) != "-"){
            val responseCode = splitedList(8).toInt
            val contentSize = splitedList(9).toLong
            //(ipAdress,clientIdentd,userId,dateTime,method,endpoint,protocol,responseCode,contentSize)
            new ApacheAccessLog(ipAdress,clientIdentd,userId,dateTime,method,endpoint,protocol,responseCode,contentSize)
          }
        }catch {
          case e: Exception => e.printStackTrace()
        }
      }else{
        println("日志格式不合法")
      }
    })

    // output
    splitedRDD.coalesce(1)
      .foreachPartition(_.foreach(println))

    // sleep
    Thread.sleep(10000000000L)
    // stop
    sc.stop()
  }
}