package com.atguigu.bigdata.different

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlTest {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 数据库的连接配置
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "123456"


    // 读取Mysql的数据
    //    val jdbc = new JdbcRDD(
    //      sc,
    //      ()=>{
    //        Class.forName(driver)
    //        DriverManager.getConnection(url, userName, passWd)
    //      },
    //      "select * from user where id >= ? and id <= ?",
    //      1,
    //      3,
    //      3,//这三个参数都是用来防止数据重复出现，因为是在不同的分区
    //      (rs)=>{
    //        println(rs.getString("name") + ":" + rs.getInt("age"))
    //      }
    //    )
    //    jdbc.collect()
    // 向Mysql写入数据
    // 1000
    // 向Mysql写入数据
    // 1000
    val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))
    // 所有的连接对象没有办法序列化——如果序列化会造成所有的都可以访问，不安全
    // foreachPartition类似于mapPartitions——在算子中，都在Executor执行，不需要序列化
    //以分区为单位进行创建连接，大大减少了创建连接的次数
    dataRDD.foreachPartition(datas=>{
      // Executor Coding
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = "insert into user(id, name, age) values (?, ?, ?)"
      val pstat: PreparedStatement = connection.prepareStatement(sql)

      datas.foreach{
        case (id, name, age) => {
          // Executor
          // 操作数据库
          pstat.setInt(1, id)
          pstat.setString(2, name)
          pstat.setInt(3, age)
          pstat.executeUpdate()
        }
      }
      pstat.close()
      connection.close()
    })

    sc.stop()

  }

}
