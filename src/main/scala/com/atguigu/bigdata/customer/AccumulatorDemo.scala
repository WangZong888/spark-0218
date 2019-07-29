package com.atguigu.bigdata.customer


import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    //声明累加器
    //注册累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach {
      num =>
        //使用累加器
        sum.add(num)
    }
    //访问累加器
    println(sum.value)
    sc.stop()
  }
}

