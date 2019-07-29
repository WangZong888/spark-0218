package com.atguigu.bigdata.customer

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.util.HashSet

object CustomerAccumuDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyAccumulator").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val list = List("Spark","Hello","Flume","Hadoop","Kafka","Hbase")
    val rdd: RDD[String] = sc.makeRDD(list,2)

      //创建累加器
    val acc = new CustomerAccumulator
    //注册累加器
    sc.register(acc,"MyAccumulator")
    //操作累加器
    rdd.foreach{
      word => acc.add(word)
    }
    //访问累加器
    println(acc.value)

  }

}
//自定义累加器
//将不合法的单词抽取出来
class CustomerAccumulator extends AccumulatorV2[String,HashSet[String]]{

  //将不合法的单词放在集合中，这是用的是java的工具包
  val backNameset: HashSet[String] = new HashSet[String]()

  //是否为初始化 顺序是 copy()->reset()->isZero()
  override def isZero: Boolean = {
    backNameset.isEmpty
  }
  //复制累加器
  override def copy(): AccumulatorV2[String, HashSet[String]] = {
    new CustomerAccumulator
  }
  //重置累加器
  override def reset(): Unit = {
    backNameset.clear()
  }

  //增加数据-根据需要的规则进行添加
  override def add(word: String): Unit = {
    if(word.contains("H")){
      backNameset.add(word)
    }

  }

  //合并累加器，因为有不同的Executor，需要最终合并在一起
  override def merge(other: AccumulatorV2[String, HashSet[String]]): Unit = {
    backNameset.addAll(other.value)
  }

  //获取累加器的值
  override def value: HashSet[String] ={
    backNameset
  }
}
