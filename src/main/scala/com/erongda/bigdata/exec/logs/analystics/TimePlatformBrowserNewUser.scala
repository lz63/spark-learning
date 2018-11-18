package com.erongda.bigdata.exec.logs.analystics

/**
  * Created by fuyun on 2018/10/11.
  */
case class TimePlatformBrowserNewUser(
                                       uuid: String,
                                       day: Int,
                                       platformDimension: String,
                                       browserDimension: String
                                     ) {
  object TimePlatformBrowserNewUser{
    /*def toDF(line: Tuple4[String,Int,String,String]){
      TimePlatformBrowserNewUser(
        line._1,
        line._2,
        line._3,
        line._4
      )
    }*/
  }
}
