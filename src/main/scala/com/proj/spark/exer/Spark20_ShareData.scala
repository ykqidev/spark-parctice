package com.proj.spark.exer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_ShareData {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // val i:Int = dataRDD.reduce(_+_)
    // println(i)
    var sum: Int = 0
    // 使用累加器来共享变量，来累加数据

    // 创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator

    dataRDD.foreach {
      case i => {
        // 执行累加器的累加功能
        accumulator.add(i)
      }
    }

    // 获取累加器的值
    println("sum = " + accumulator.value)

    sc.stop()


  }
}

