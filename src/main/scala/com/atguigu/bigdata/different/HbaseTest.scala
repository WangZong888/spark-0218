package com.atguigu.bigdata.different

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HbaseTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HbaseRDD").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    /* //读取HBase的数据
     //构建HBase配置信息
     val configuration: Configuration = HBaseConfiguration.create()
     // conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
     configuration.set(TableInputFormat.INPUT_TABLE, "student")

     //从HBase读取数据形成RDD
     val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
       configuration,
       classOf[TableInputFormat],
       classOf[ImmutableBytesWritable],
       classOf[Result])

     //对hbaseRDD进行处理
     hbaseRDD.foreach {
       case (rk, result) => {
         for (cell <- result.rawCells()) {
           println(Bytes.toString(CellUtil.cloneValue(cell)))
         }
       }
     }*/

    //向HBase写入数据
    val configuration: Configuration = HBaseConfiguration.create()
    val rdd: RDD[(String, String)] = sc.makeRDD(List(("2001", "XXXX"), ("2002", "YYYY"), ("2003", "ZZZZ")))

    val job: JobConf = new JobConf(configuration)
    job.setOutputFormat(classOf[TableOutputFormat])
    job.set(TableOutputFormat.OUTPUT_TABLE,"student")

    //将原始数据进行转化
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map {
      case (rk, name) => {
        val rowkey: Array[Byte] = Bytes.toBytes(rk)
        val put = new Put(rowkey)
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        (new ImmutableBytesWritable(rowkey), put)
      }
    }
    putRDD.saveAsHadoopDataset(job)
    //关闭连接
    sc.stop()

  }
}
