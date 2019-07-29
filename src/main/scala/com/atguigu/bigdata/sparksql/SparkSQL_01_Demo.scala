package com.atguigu.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_01_Demo {

  def main(args: Array[String]): Unit = {
    //配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSql").setMaster("local[*]")
    //准备环境对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //DataFrame的创建
    val df: DataFrame = spark.read.json("input/text.json")
    //创建临时表
  /*  df.createOrReplaceTempView("user")
    //通过SQL语法访问DF
    val result: DataFrame = spark.sql("select * from user")
    //访问数据
    result.show()*/

    //通过DSL（domain specific language）领域特定语言——语法访问DF
    df.select("name","age").show()


    //释放资源
    spark.stop()


  }

}
