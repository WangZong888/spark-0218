package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingUpdateState {

  def main(args: Array[String]): Unit = {

    //准备环境信息
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    //创建上下文环境对象
    val streaming: StreamingContext = new StreamingContext(conf, Seconds(3))

    //设置检查点的路径
    streaming.sparkContext.setCheckpointDir("checkpoint")
    //使用socket端口创建
    val lineDS: ReceiverInputDStream[String] = streaming.socketTextStream("hadoop102", 9999)
    //val result: DStream[(String, Int)] = lineDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //聚合函数
    //reduceByKey:无状态数据操作，只对当前数的RDD中数据有效，无法对多个采集周期的数据进行统计
    //updateStateByKey:有状态的数据操作：需要设定检查点目录，然后将状态保存到检查点中
    //第①个参数是周期中所有key相同的value的集合，第②个参数是每个周期计算并存放数据的缓冲区，返回的缓冲区的计算数据
    val result: DStream[(String, Long)] = lineDS.flatMap(_.split(" ")).map((_, 1)).updateStateByKey[Long] {
      (valSeq: Seq[Int], buffer: Option[Long]) =>
        val sum: Long = buffer.getOrElse(0L) + valSeq.sum
        Option(sum)
    }

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
