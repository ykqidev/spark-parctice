package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Sample {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    // 从指定的数据集合中进行抽样处理，根据不同的算法进行抽样
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // withReplacement表示是抽出的数据是否放回，
    // true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子
//        val sampleRDD: RDD[Int] = listRDD.sample(true, 0.4, 1)
    val sampleRDD: RDD[Int] = listRDD.sample(true, 4, 1)

    println(sampleRDD.collect().mkString(","))

  }
}
