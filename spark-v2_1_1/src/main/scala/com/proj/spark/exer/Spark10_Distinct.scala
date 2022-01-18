package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Distinct {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 3, 2, 1, 5, 6, 4, 6, 5))

    //    val distinctRDD: RDD[Int] = listRDD.distinct()
        // 使用distinct算子对数据去重，但是因为去重后会导致数据减少，所以可以改变默认的分区的数量
        val distinctRDD: RDD[Int] = listRDD.distinct(2)

//    println(distinctRDD.collect().mkString(","))

    distinctRDD.saveAsTextFile("output")
  }
}
