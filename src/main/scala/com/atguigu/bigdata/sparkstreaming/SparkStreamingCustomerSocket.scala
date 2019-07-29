package com.atguigu.bigdata.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingCustomerSocket {

  def main(args: Array[String]): Unit = {

    //准备环境信息
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //创建上下文环境对象
    val streaming: StreamingContext = new StreamingContext(conf, Seconds(3))

    //使用自定义采集器socket端口创建
    val receiver = new MyReceiver("hadoop102",9999)
    val receiverDS: ReceiverInputDStream[String] = streaming.receiverStream(receiver)
    val result: DStream[(String, Int)] = receiverDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

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
//自定义采集器Receiver
//1)继承Receiver
//2)重写onStart onStop
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2){

  private var socket:Socket = _

  //接收数据
  def receive(): Unit ={
    try{
      socket = new Socket(host,port)
    }catch {
      case e:ConnectException => return
    }
    //接收数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,"UTF-8"))
    var line = ""
    while((line = reader.readLine()) !=null){
      if(line=="===End==="){//scala中字符串==比较的是内容
        return
      }else{
        //将数据转换成DStream
        store(line)
      }
    }
  }

  override def onStart(): Unit ={
      new Thread("Socket Receiver"){
        override def run() = {receive()}
      }.start()
  }

  override def onStop(): Unit = {
    if(socket != null){
      socket.close()
      socket = null
    }
  }
}
