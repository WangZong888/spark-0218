package com.atguigu.bigdata.sparksql


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object AvgUDAFDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyUDAF").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"xxxx",20),(2,"yyyy",30),(3,"zzzz",40)))

    //记得添加隐式类型转换
    import spark.implicits._
    val df: DataFrame = dataRDD.toDF("id","name","age")
    df.createOrReplaceTempView("user")

    //创建自定义函数
    val avgAge = new MyAvgUDAF
    //向spark注册
    spark.udf.register("AvgXxx",avgAge)
    //使用聚合函数
    spark.sql("select AvgXxx(age) avga from user").show

    spark.stop()
  }

}
class MyAvgUDAF extends UserDefinedAggregateFunction{

  //输入数据结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  //缓冲区的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //聚合函数的结果类型
  override def dataType: DataType = {
    DoubleType
  }

  //相同的数据是否每次运算后都一样——当前函数时候稳定
  override def deterministic: Boolean = {
    true
  }

  //缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =buffer.getLong(0)+input.getLong(0)
    buffer(1) = buffer.getLong(1)+1L
  }

  //合并缓冲区数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }

  //计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}