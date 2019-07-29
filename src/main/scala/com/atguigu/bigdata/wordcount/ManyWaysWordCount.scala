package com.atguigu.bigdata.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ManyWaysWordCount {

  def main(args: Array[String]): Unit = {

    //数据准备
    val stringList: List[(String, Int)] = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
    //使用scala实现
    //1)最基础的一种
    val tuple1: List[(String, Int)] = stringList.map(t => (t._1 + " ") * t._2).flatMap(_.split(" ")).
      groupBy(word => word).map(t => (t._1, t._2.size)).toList.sortWith(_._2 > _._2).take(3)
    println("This Is First Method With Scala: " + tuple1)

    //2)换种思路进行编写(words,count) =>(word,count)
    val tuple2: List[(String, Int)] = stringList.flatMap(t => t._1.split(" ").map((_, t._2))).groupBy(_._1).
      map(t => (t._1, t._2.map(_._2).sum)).toList.sortWith(_._2 > _._2).take(3)
    println("This Is Second Method With Scala: " + tuple2)

    //对第二种的另一种简化写法-mapValues()拿到的是每一个v
    val tuple2_1 = stringList.flatMap(t => t._1.split(" ").map((_, t._2))).groupBy(_._1).mapValues(_.map(_._2).sum).
      toList.sortWith(_._2 > _._2).take(3)
    println("This Is Second OtherMethod With Scala: " + tuple2_1)

    //使用Spark实现
    //先准备环境
    val conf: SparkConf = new SparkConf().setAppName("WordCountMethods").setMaster("local")
    val context: SparkContext = new SparkContext(conf)

    //3)使用ReduceByKey算子
    //    val tuple3: Array[(String, Int)] = context.makeRDD(stringList, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
    //      reduceByKey(_ + _).collect()
    //
    //    println("This Is Third Method With Spark: ")
    //    tuple3.sortWith(_._2 >_._2).take(3).foreach(println)

    //4)使用aggregateByKey算子
    //    val tuple4: Array[(String, Int)] = context.makeRDD(stringList, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
    //      aggregateByKey(0)(_ + _, _ + _).collect()
    //    println("This Is Fourth Method With Spark: ")
    //    tuple4.sortWith(_._2 > _._2).take(3).foreach(println)

    //5)使用foldByKey算子--是aggregateByKey的简写，必须是分区内和分区间的逻辑运算一致
    //    val tuple5: Array[(String, Int)] = context.makeRDD(stringList, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
    //      foldByKey(0)(_ + _).collect()
    //
    //    println("This Is Fifth Method With Spark: ")
    //    tuple5.sortWith(_._2 >_._2).take(3).foreach(println)

    //6)使用combineByKey算子
    val tuple6: Array[(String, Int)] = context.makeRDD(stringList, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
      combineByKey(v => v, (t: Int, next) => (t + next), (t1: Int, t2: Int) => (t1 + t2)).collect()

    println("This Is Sixth Method With Spark: ")
    tuple6.sortWith(_._2 > _._2).take(3).foreach(println)
    //关闭
    context.stop()


  }
}
