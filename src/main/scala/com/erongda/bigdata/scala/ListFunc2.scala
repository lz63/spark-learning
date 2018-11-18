package com.erongda.bigdata.scala

import scala.collection.mutable.ListBuffer

/**
  * Scala中列表List中高阶函数讲解案例二
  */
object ListFunc2 {

  def main(args: Array[String]): Unit = {

    // 创建List集合
    val list: List[Int] = List(1, 2, 7, 8, 3, 4, 9, 10, 5, 6)

    // TODO: 复习 reduce函数
    //  def reduce[A1 >: A](op: (A1, A1) => A1): A1 = reduceLeft(op)
    // def reduceLeft[B >: A](op: (x1: B, x2: A) => B): B
    list.reduceLeft((tmp, item) => {
      println(s"tmp = $tmp, item = $item")
      tmp + item
    })

    // TODO
    /**
      * def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): A1 = foldLeft(z)(op)
      * 参数说明：
      *   -a. 第一个参数：z
      *     表明的是Zero，初始化，聚合函数聚合过程中中间临时变量的初始化
      *   -b. 第二个参数：op: (x1: A1, x2: A1) => A1
      *     x1 表示的是 中中间临时变量 的值
      *     x2 表示的是 集合个每个元素
      */
    // 求和，此处使用fold函数大材小用
    list.fold(0)((tmp, item) => {
      println(s"tmp = $tmp, item = $item")
      tmp + item
    })

    // 求取最大的三个值: 将数据放到集合中，进行降序排序，获取前3个最大值
    // def foldLeft[B](z: B)(op: (B, A) => B): B
    list.foldLeft(ListBuffer[Int]())((tmp, item) => {
      tmp += item
      println(s"tmp = $tmp")
      tmp.sorted.takeRight(3).reverse
    })

    /**
      * 对集合中的数据去重
      */
    val lst = List(11, 22, 11, 11, 22, 99, 100)
    // TODO： 使用foldLeft/foldRight函数对集合中数据进行去重
    import scala.collection.mutable
    lst.foldLeft(mutable.Set[Int]())((set, item) => {
      println(s"set = $set, item = $item")
      set += item
    })


    lst.foldRight(mutable.Set[Int]())((item, set) => {
      println(s"set = $set, item = $item")
      set += item
    })

    /**
      * def aggregate[B](z: =>B)(seqop: (B, A) => B, combop: (B, B) => B): B = foldLeft(z)(seqop)
      */
    //list.aggregate()

    // TODO:
    /**
      * 集合的合并（压缩）操作：
      *   将两个集合合并为一个集合，新的集合的数据类型为二元组（Key，Value）对
      */
    val list1 = List(1, 2, 3, 4, 5)
    val list2 = List("A", "B", "C", "D", "E")
    val zipList: List[(Int, String)] = list1.zip(list2)

    // 可以将一个List[(Key, Value)]拆分为两个List
    val (keys, values) = zipList.unzip


    val map = Map(11 -> "A", 22 -> "BB", 33 -> "CC")
    val (ks, vs) = map.unzip


    /**
      * 集合的拉链操作：
      *   将集合中的元素转换（Key, Value）对，Key为元素的值，Value为元素在原先集合中下标值
      */
    val lt = List("AA", "BB", "CC", "DD", "EE")
    // 拉链操作
    val zipIndexList: List[(String, Int)] = lt.zipWithIndex
  }

}
