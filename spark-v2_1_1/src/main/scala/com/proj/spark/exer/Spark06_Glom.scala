package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Glom {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,3,6,5,8,6,2,4),3)

    // 将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array=>{
      println(array.mkString(","))
    })
  }
}
