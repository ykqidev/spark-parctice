package com.proj.spark.exer

import com.proj.spark.utils.FileUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Coalesce {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    println("缩减分区前" + listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减分区后" + coalesceRDD.partitions.size)
    FileUtil.deleteFile("output")
    coalesceRDD.saveAsTextFile("output")
  }
}
