package com.proj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {

  def main(args: Array[String]): Unit = {
    // SparkSQL

    // SparkConf
    // 创建配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    // SparkContext
    // SparkSession
    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取数据，构建DataFrame
    val frame: DataFrame = spark.read.json("in/json/user.json")

    // 展示数据
    frame.show()

    // 释放资源
    spark.stop()
  }
}
