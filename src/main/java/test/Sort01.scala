package test

import java.util.{Calendar, Date}

/**
  * 对无序集合进行降序排序
  */
object Sort01 {

  def main(args: Array[String]): Unit = {

    val arr1 = Array(29, 54, 61, 12, 94, 3, 16)
//    val startDate = Calendar.getInstance().getTimeInMillis
//    println(s"===============${startDate}=====================")
    val startTime: Long = System.currentTimeMillis()
    println(s"===============${startTime}=====================")
    println(s"排序前的数组： ${arr1.mkString("[", ", ", "]")}")
    var temp = 0
    for (i <- 0 until arr1.length) {
      for (j <- 0 until(arr1.length - i - 1)) {
        if (arr1(j) < arr1(j + 1)) {
          temp = arr1(j)
          arr1(j) = arr1(j + 1)
          arr1(j + 1) = temp
        }
      }
    }
    println(s"排序后的数组： ${arr1.mkString("[", ", ", "]")}")
//    val endDate = Calendar.getInstance().getTimeInMillis
//    println(s"===============${endDate}=====================")
    val endTime: Long = System.currentTimeMillis()
    println(s"===============${endTime}=====================")
    println(s"=============运行时间：${endTime - startTime}=====================")






  }
}
