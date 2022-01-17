package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_FlatMap {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc: SparkContext = new SparkContext(config)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 5)))

    val flatMapRDD: RDD[Int] = listRDD.flatMap(data => data)

    println(flatMapRDD.collect().mkString(","))

  }
}
