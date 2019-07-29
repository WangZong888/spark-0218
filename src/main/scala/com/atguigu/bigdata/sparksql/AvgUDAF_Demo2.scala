package com.atguigu.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


object AvgUDAF_Demo2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyUDAF").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        //记得添加隐式类型转换
    import spark.implicits._
    //创聚合函数
    val udaf = new MyAvgAgeUDAF
    //将聚合函数转换为查询列
    val col: TypedColumn[User1, Double] = udaf.toColumn.name("avgAge")

    val df: DataFrame = spark.read.json("input/text.json")
    //使用DSL语法访问强类型聚合函数（面向对象）  || 弱类型使用SQL语法
    val ds: Dataset[User1] = df.as[User1]

    ds.select(col).show()

    spark.stop()
  }

}

case class User1(name: String, age: Long)

case class AvgBuff(var total: Long, var count: Long)

class MyAvgAgeUDAF extends Aggregator[User1, AvgBuff, Double] {

  //初始化缓冲区数据
  override def zero: AvgBuff = {
    AvgBuff(0L, 0L)
  }

  //将每条数据聚合
  override def reduce(buffer: AvgBuff, input: User1): AvgBuff = {
    buffer.total = buffer.total + input.age
    buffer.count = buffer.count + 1L
    buffer
  }

  //将缓冲区之间的数据进行合并
  override def merge(buffer1: AvgBuff, buffer2: AvgBuff): AvgBuff = {
    buffer1.total = buffer1.total + buffer2.total
    buffer1.count = buffer1.count + buffer2.count
    buffer1
  }

  //完成计算
  override def finish(reduction: AvgBuff): Double = {
    reduction.total.toDouble / reduction.count
  }

  //下面2个是固定写法
  override def bufferEncoder: Encoder[AvgBuff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
