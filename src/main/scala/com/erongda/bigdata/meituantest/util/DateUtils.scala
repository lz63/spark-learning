package com.erongda.bigdata.meituantest.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * 时间相关的数据格式工具类
  */
object DateUtils {

  /**
    * 按照给定的毫秒级时间戳以及格式化字符串进行日期数据格式化
    * @param time
    *             Long类型时间格式
    * @param pattern
    *                格式化样式
    * @return
    */
  def parseLong2String(time: Long, pattern: String): String = {
    // 创建Calendar实例对象
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)
    // 格式转换
    new SimpleDateFormat(pattern).format(cal.getTime)
  }

}
