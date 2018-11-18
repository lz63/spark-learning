package com.erongda.bigdata.project.etl

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
  * 读取properties文件得到链接MySQL的属性链接MySQL及关闭链接
  */
object ConnectionUtils {
  def getConn(): Connection = {
    // 读取properties文件
    val inputStream = ConnectionUtils.getClass.getClassLoader.getResourceAsStream("jdbc.properties")
    // 实例化properties对象
    val prop = new Properties()
    // 加载properties文件
    prop.load(inputStream)
    // a. Connection -> 运行在Executor上
    Class.forName(prop.getProperty("driver.class.name"))
    val url = prop.getProperty("mysql.url")
    val username = prop.getProperty("mysql.user")
    //    print("请输入用户密码：")
    //    import java.util.Scanner
    //    val scanner = new Scanner(System.in)
    //    val password = scanner.next()
    val password = prop.getProperty("mysql.password")
    var conn: Connection = null
    try{
      //  获取数据库的连接
      conn = DriverManager.getConnection(url, username, password)
    }catch {
      case e:Exception => println("MySQL Exception")
    }
    conn
  }

  def closeConnection(conn:Connection): Unit = {
    if(conn != null){
      conn.close()
    }
  }
}
