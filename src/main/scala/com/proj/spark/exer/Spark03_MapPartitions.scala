package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_MapPartitions {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartitions可以对一个RDD中所有的分区进行遍历
    // mapPartitions 效率优于map算子，减少了发送到执行器执行交互次数
    // mapPartitions 可能会出现内存溢出（OOM）
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(data => {
      data.map(data => data * 2)
    })

    println(mapPartitionsRDD.collect().mkString(","))

  }
}
