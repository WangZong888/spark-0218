package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 算子 - aggregateByKey
    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

    // combineByKey需要传递三个参数



    // 第一个参数：转换数据的结构 88 => (88,1)
    // 第二个参数：分区内计算规则
    // 第三个参数：分区间计算规则
    val resultRDD: RDD[(String, (Int, Int))] = input.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t: (Int, Int), t1: (Int, Int)) => {
        (t._1 + t1._1, t._2 + t1._2)
      }
    )
    resultRDD.map{
//      case (k, v) => {
//        (k, v._1/v._2)
//      }
      t => (t._1,t._2._1/t._2._2)
    }.collect().foreach(println)

    sc.stop()
  }
}
