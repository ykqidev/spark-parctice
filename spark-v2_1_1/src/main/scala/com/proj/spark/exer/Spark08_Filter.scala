package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Filter {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    // 生成数据，按照指定的规则进行分组
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6))

    val filterRDD: RDD[Int] = listRDD.filter(_ % 2 == 0)

    println(filterRDD.collect().mkString(","))

  }
}
