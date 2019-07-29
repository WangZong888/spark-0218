package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // 使用Spark 计算框架完成第一案例：WordCount

    // 创建Spark配置对象
    // setMaster : 设定当前Spark的运行环境，取值为local,表示本机环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")

    // 创建Spark上下文环境对象
    // 默认的部署模式为local
    val context: SparkContext = new SparkContext(conf)

    // 读取文件:可以采用相对路径指向IDEA工程中的目录
    // textFile方法的作用是读取文件目录(文件)
    // 如果文件存储在HDFS上，那么路径应该使用HDFS格式
    val line: RDD[String] = context.textFile("input")
    // 将每一行的字符串拆分成一个一个的单词
    // 扁平化
    //lineRDD.flatMap(line=>line.split(" "))
    val word: RDD[String] = line.flatMap(_.split(" "))
    // 将每一个单词进行结构的转换，为了统计的方便
    //wordRDD.map(word=>(word, 1))
    val wordToOne: RDD[(String, Int)] = word.map((_,1))
    // 将转换结构后的数据进行分组聚合
    //wordToOneRDD.reduceByKey((x,y)=>x+y)
    // reduceByKey方法会将数据使用key进行分组,分组后将value数据进行聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    // 采集数据
    // collect方法会将数据结果采集到内存中，形成数组
    val tuples: Array[(String, Int)] = wordToCount.collect()

    tuples.foreach(println)

    // 释放资源
    context.stop()

    //一步到位
//    val tuples: Array[(String, Int)] = context.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()
//    tuples.foreach(println)
//    context.stop()

  }
}
