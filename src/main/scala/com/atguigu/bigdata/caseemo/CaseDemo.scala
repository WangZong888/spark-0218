package com.atguigu.bigdata.caseemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CaseDemo {

  def main(args: Array[String]): Unit = {

    //TODO 需求：统计出每一个省份广告被点击次数的TOP3
    val conf: SparkConf = new SparkConf().setAppName("AdvCount").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)
    //TODO 1)拿到原始数据
    // 读取一行数据 1516609143867 6 7 64
    // 数据结构：时间戳，省份，城市，用户，广告
    val lineRDD: RDD[String] = context.textFile("input/agent.log")
    //TODO 2)对数据进行整理，拿到我们需要的数据-map
    //(省份_广告，1)
    val priAdvToOne: RDD[(String, Int)] = lineRDD.map {
      line =>
        val strings: Array[String] = line.split(" ")
        (strings(1) + "_" + strings(4), 1)
    }

    //TODO 3)将数据进行分组聚合-reduceByKye
    //(省份_广告,sum)
    val priAdvToSum: RDD[(String, Int)] = priAdvToOne.reduceByKey(_+_)
    //TODO 4)需要将数据结构进行转化-map
    //(省份,(广告,sum))
    val priToAdvSum : RDD[(String, (String, Int))] = priAdvToSum.map {
      //      t =>
      //        val strings: Array[String] = t._1.split("_")
      //        (strings(0), (strings(1), t._2))
      case (key, sum) =>
        val strings: Array[String] = key.split("_")
        (strings(0), (strings(1), sum))
    }

    //TODO 5)根据省份分组-groupByKey
    val groupByPri: RDD[(String, Iterable[(String, Int)])] = priToAdvSum.groupByKey()

    //TODO 6)将m每一个省份中的广告的点击次数进行排序（降序），并取出Top3--mapValues
    val result: RDD[(String, List[(String, Int)])] = groupByPri.mapValues {
      datas =>
        datas.toList.sortWith(_._2 > _._2).take(3)
    }
    //TODO 7)打印出来
    result.collect().foreach(println)
    context.stop()
  }
}
