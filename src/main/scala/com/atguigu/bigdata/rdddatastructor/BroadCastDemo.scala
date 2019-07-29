package com.atguigu.bigdata.rdddatastructor

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyAccumulator").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)


    // 1000万
    val listRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    // 1000万
    //val listRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    val list2 = List(("a", 4), ("b", 5), ("c", 6))

    //使用broadcast调优,通过value属性获取值
    val broadcastList2: Broadcast[List[(String, Int)]] = sc.broadcast(list2)

    //会产生shuffle
    //(a,(1,4))——这个匹配过程是一个笛卡尔过程，如果有大量的数据，性能就非常的差
//    val joinRDD: RDD[(String, (Int, Int))] = listRDD1.join(listRDD2)
//    joinRDD.foreach(println)

    //想实现上述的功能,不考虑特殊情况——可以用map,但是这种情况可能会出现OOM异常——
    // 因为变量（list2）从Driver发送给Executor端是以Task为单位的，Task之间是相互独立的，而一个Executor可能有多个Task，所以可能有
    //多份相同的数据
    //最终可以使用broadcast变量进行调优——包装一层，改变发送单位为节点，变量只会发送一次
    //在算子之中的代码是在Executor中执行，算子之外的是在Driver中执行
    val resultRDD: RDD[(Any, Any)] = listRDD1.map {
      case (k1, v1) =>
        var kk: Any = k1
        var vv: Any = null
        for ((k2, v2) <-  broadcastList2.value) {
          if (k1 == k2) {
            vv = (v1, v2)
          }
        }
        (kk, vv)
    }
    resultRDD.foreach(println)

    sc.stop()

  }
}
