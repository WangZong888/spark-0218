package com.atguigu.bigdata.different

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JsonTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JsonRDD").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    import scala.util.parsing.json.JSON
    //从hdfs上读取文件
    //sc.textFile("hdfs://hadoop102:9000/input")
    val jsonText: RDD[String] = sc.textFile("input/text.json")

    //Spark读取JSON文件是，要求每一行都要符合JSON格式,不满足返回None，满足返回Some对象
    val result = jsonText.map(JSON.parseFull)

    result.collect().foreach(println)

    sc.stop()
  }

}
