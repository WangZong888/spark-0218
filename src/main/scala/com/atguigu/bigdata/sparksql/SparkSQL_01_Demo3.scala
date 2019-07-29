package com.atguigu.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_01_Demo3 {

  def main(args: Array[String]): Unit = {
    //配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSql").setMaster("local[*]")
    //准备环境对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD、DataFrame、DataSet之间的相互转换时，需要隐式转换规则
    import spark.implicits._ //标记为灰色，表示没用上,spark是变量名——是SparkSession对象名称
    val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40), (3, "wangwu", 50)))

   //RDD ==> DataSet
    //样例类同时拥有结构和类型，可以将RDD的数据转换为对象后，在转换为DataSet
    val userRDD: RDD[User] = dataRDD.map {
      case (id, name, age) => User(id, name, age)
    }
    val ds: Dataset[User] = userRDD.toDS()
    //ds.show()

    //DataSet => RDD
    val rdd: RDD[User] = ds.rdd
    rdd.foreach{
      user => println(user.id+","+user.name+","+user.age)
    }


    //释放资源
    spark.stop()
  }
}
