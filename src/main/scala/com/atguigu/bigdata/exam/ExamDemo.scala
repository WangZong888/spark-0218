package com.atguigu.bigdata.exam


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ExamDemo {

  def main(args: Array[String]): Unit = {
    val stringLists: List[(String, Int)] = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))


    //1)
    val tuple2: List[(String, Int)] = stringLists.map(t => (t._1 + " ") * t._2).flatMap(_.split(" ")).groupBy(word => word).map(t => (t._1, t._2.size)).
      toList.sortWith(_._2 > _._2).take(3)
    println(tuple2)
    //2)
    val tuple1: List[(String, Int)] = stringLists.flatMap(t => t._1.split(" ").map((_, t._2))).groupBy(_._1).map(t => (t._1, t._2.map(_._2).sum)).
      toList.sortWith(_._2 > _._2).take(3)
    println(tuple1)

    //3)
    val tuple3: List[(String, Int)] = stringLists.flatMap(t => t._1.split(" ").map((_, t._2))).groupBy(_._1).mapValues(_.map(_._2).sum).
      toList.sortWith(_._2 > _._2).take(3)
    println(tuple3)

    //准备工作
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    /*//4）reduceByKey
    val tuple4: Array[(String, Int)] = context.makeRDD(stringLists, 2).flatMap(t => t._1.split(" ").map((_, t._2))).reduceByKey(_ + _).
      collect().sortWith(_._2 > _._2).take(3)
    tuple4.foreach(println)

    //5)aggregateByKey
    val tuple5: Array[(String, Int)] = context.makeRDD(stringLists, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
      aggregateByKey(0)(_ + _, _ + _).collect().sortWith(_._2 > _._2).take(3)

    tuple5.foreach(println)

    //6)foldByKey
    val tuple6: Array[(String, Int)] = context.makeRDD(stringLists, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
      foldByKey(0)(_ + _).collect().sortWith(_._2 > _._2).take(3)
    tuple6.foreach(println)

    //7)combineByKey
    val tuple7: Array[(String, Int)] = context.makeRDD(stringLists, 2).flatMap(t => t._1.split(" ").map((_, t._2))).
      combineByKey(v => v, (t: Int, v) => (t + v), (t: Int, t1: Int) => (t + t1)).collect().sortWith(_._2 > _._2).take(3)
    tuple7.foreach(println)

    //8)groupBy
    val tuple8: Array[(String, Int)] = context.makeRDD(stringLists, 3).flatMap(t => t._1.split(" ").map((_, t._2))).
      groupBy(_._1).map(t => (t._1, t._2.map(_._2).sum)).collect().sortWith(_._2 > _._2).take(3)
    tuple8.foreach(println)

    //9)groupByKey
    val tuple9: Array[(String, Int)] = context.makeRDD(stringLists, 3).flatMap(t => t._1.split(" ").map((_, t._2))).
      groupByKey().map(t => (t._1, t._2.sum)).collect().sortWith(_._2 > _._2).take(3)

    tuple9.foreach(println)
*/
    //10)行动Action——countByKey算子
   /* val tuples: RDD[(String, Int)] = context.makeRDD(stringLists, 3).flatMap(t => t._1.split(" ").map((_, t._2))).flatMap { t =>

      var list = scala.collection.mutable.ArrayBuffer[(String,Int)]()
      var i = t._2
      while (i > 0) {
        list.+=((t._1,1))
        i -= 1
      }
      list
    }
    val t1: List[(String, Long)] = tuples.countByKey().toList.sortWith(_._2 > _._2).take(3)

  t1.foreach(println)*/




    val dataRDD: RDD[(String, Int)] = context.makeRDD(List(("a",2), ("a", 3), ("a", 4)))

    val stringToLo: collection.Map[String, Long] = dataRDD.countByKey()

    println(stringToLo)

    val strRDD: RDD[String] = context.makeRDD(List("Hello", "Hello", "Hello", "Scala"))

    val stringToLong1: collection.Map[String, Long] = strRDD.countByValue()
    println(stringToLong1)

    context.stop()






  }

}
