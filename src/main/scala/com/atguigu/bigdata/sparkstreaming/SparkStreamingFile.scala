package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingFile {

  def main(args: Array[String]): Unit = {

    //准备环境信息
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    //创建上下文环境对象
    val streaming: StreamingContext = new StreamingContext(conf, Seconds(3))

    //使用file创建
    val fileDS: DStream[String] = streaming.textFileStream("hdfs://hadoop102:9000/input")
    val result: DStream[(String, Int)] = fileDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //直接打印
    result.print()

    //释放资源
    //SparkStreaming的采集器需要长期执行，不能停止
    //SparkStreaming的采集器需要明确确定
    streaming.start()

    //Driver程序不能单独停止，需要等待采集器执行结束

    streaming.awaitTermination()


  }
}
