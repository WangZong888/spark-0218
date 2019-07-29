package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWindow {

  def main(args: Array[String]): Unit = {

    //准备环境信息
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    //创建上下文环境对象
    val streaming: StreamingContext = new StreamingContext(conf, Seconds(3))

    //使用socket端口创建
    // 使用窗口函数对多个采集周期的数据进行统计
    val lineDS: ReceiverInputDStream[String] = streaming.socketTextStream("hadoop102", 9999)
    //将采集数据放置在窗口中
    val windowStream: DStream[String] = lineDS.window(Seconds(9),Seconds(3))
    val result: DStream[(String, Int)] = windowStream
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

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
