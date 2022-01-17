package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Checkpoint {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 设定检查点的保存目录
    sc.setCheckpointDir("cp")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.checkpoint()

    println(reduceRDD.toDebugString)

    // 一定要执行Action checkpoint才能生效
    reduceRDD.foreach(println)

    sc.stop()
  }
}

