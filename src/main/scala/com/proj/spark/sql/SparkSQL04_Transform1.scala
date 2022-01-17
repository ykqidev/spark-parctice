package com.proj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSQL04_Transform1 {

  def main(args: Array[String]): Unit = {
    // SparkSQL

    // SparkConf
    // 创建配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    // SparkContext
    // SparkSession
    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._
    // 创建RDD
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("zhouba", 23), ("wujiu", 32)))

    // RDD - DataSet
    val userRDD: RDD[User] = rdd.map {
      case (name, age) => {
        User(name, age)
      }
    }

    val userDS: Dataset[User] = userRDD.toDS()

    val rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)

    // 释放资源
    spark.stop()
  }
}

//case class User(name: String, age: Int)