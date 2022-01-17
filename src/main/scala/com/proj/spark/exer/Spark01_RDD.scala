package com.proj.spark.exer

import com.proj.spark.utils.FileUtil.deleteFile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Exer")

    // 创建Spark上下文对象
    val sc = new SparkContext(config)

    // 创建RDD
    // 1) 从内存中创建 makeRDD
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //    val arrRDD: RDD[String] = sc.makeRDD(Array("1", "2", "3"))

    // 2) 从内存中创建 parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6))

    // 3) 从外部存储中创建
    // 默认情况下，可以读取项目路径
    // 默认从文件中读取的数据都是字符串类型
//    val fileRDD: RDD[String] = sc.textFile("in/json/")
//    println(fileRDD.collect().mkString(","))

    deleteFile("output")
    listRDD.saveAsTextFile("output")


  }
}
