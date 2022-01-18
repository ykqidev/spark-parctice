package com.proj.spark.exer

import java.sql.{Connection, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_HBase {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val conf: Configuration = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "student")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.foreach {
      case (writable, result) => {
        val cells: Array[Cell] = result.rawCells()
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }

    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1002", "zhangsan"), ("1003", "wangwu")))

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, name) =>
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")
    putRDD.saveAsHadoopDataset(jobConf)


    sc.stop()


  }
}

