package com.atguigu.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_01_Demo2 {

  def main(args: Array[String]): Unit = {
    //配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSql").setMaster("local[*]")
    //准备环境对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD、DataFrame、DataSet之间的相互转换时，需要隐式转换规则
    import spark.implicits._ //标记为灰色，表示没用上,spark是变量名——是SparkSession对象名称
    val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40), (3, "wangwu", 50)))

    //在RDD基础上增加结构信息，转化成DataFrame
    val df: DataFrame = dataRDD.toDF("id", "name", "age")
    //在DataFrame基础上增加类型信息，转换成DS
    val ds: Dataset[User] = df.as[User]
    //显示数据
    //ds.show()

    //将DataSet转换成DataFrame
    val df1: DataFrame = ds.toDF()
//    df1.show()

    //将DataFrame转换成RDD
    val dataRDD1: RDD[Row] = df1.rdd
    dataRDD1.foreach(row => println(row.getInt(0),row.getString(1),row.getInt(2)))
    //释放资源
    spark.stop()
  }
}

//转换成DataSet需要类型，创建样例类
case class User(id: Long, name: String, age: Long)