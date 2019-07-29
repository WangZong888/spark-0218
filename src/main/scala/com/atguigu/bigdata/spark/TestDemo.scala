package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val context: SparkContext = new SparkContext(conf)


    // 读取文件:可以采用相对路径指向IDEA工程中的目录
    // textFile方法的作用是读取文件目录(文件)
    // 如果文件存储在HDFS上，那么路径应该使用HDFS格式
    //val unit: RDD[String] = context.textFile("input/1.txt",3)
    //unit.saveAsTextFile("output")
    //List(1,2,List(4,5,),3)——>12453
  /* val rdd : RDD[Any] = context.makeRDD(List(1,2,List(4,5,6),3),2)
    rdd.flatMap{
      t => {
        t match{
          case i: Int => List(i)
          case b: List[Int] => b
        }
      }
    }.foreach(println)*/
/*   val value: RDD[String] = context.makeRDD(List("1","2","3"))
    value.flatMap(word => word).foreach(println)*/

    val value1: RDD[Char] = context.makeRDD("123434343sdss")
    value1.flatMap(word => Seq(word)).foreach(println)
    context.stop()
  }
}
