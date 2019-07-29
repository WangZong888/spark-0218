package com.atguigu.bigdata.sparkstreaming

import java.net.URI

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreamingKafka {

  def main(args: Array[String]): Unit = {

    //准备环境信息
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[4]")
    //开启优雅关闭-他不会立即关闭，已经拉取的数据进行处理完后在关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //创建上下文环境对象
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //将kafka基本参数映射成map
    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "consumerSpark",
      "zookeeper.connect" -> "hadoop102:2181" //注意旧版本没有新特性，不能使用BOOTSTRAP，使用zookeeper服务地址链接
    )
    //topic配置
    val topic: Map[String, Int] = Map[String, Int]("spark" -> 3)
    //用KafkaUtils工具来创建kafkaDStream，从kafka中获取数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      streamingContext, kafkaParams,topic , StorageLevel.MEMORY_AND_DISK_SER_2)

    //拿到的是一对（k,v），我们只需要v，这里进行转换格式map
    val kafkaValue: DStream[String] = kafkaDStream.map {
      case (k, v) => v
    }

    //进行wordCount求和
    val result: DStream[(String, Int)] = kafkaValue.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {
        while(true) {
          try{
            Thread.sleep(5000)
          }catch{
            case ex: Exception => println("YYYYY")
            case _ => println("xxxxx")
          }
          //监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"atguigu")
          val state: StreamingContextState = streamingContext.getState()
          //如果环境对象处于活动状态，可以进行关闭操作
          if(state == StreamingContextState.ACTIVE){
            //判断路径是否存在
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/stopspark02"))
            if(flg){
              println("000000------aaaaaa")
              streamingContext.stop(true,true)
              System.exit(0)//退出线程
            }
          }
        }
      }
    }).start()
    //打印数据
    result.print()


    //释放资源
    //SparkStreaming的采集器需要长期执行，不能停止
    //SparkStreaming的采集器需要明确确定
    streamingContext.start()

    //Driver程序不能单独停止，需要等待采集器执行结束

    streamingContext.awaitTermination()


  }
}
