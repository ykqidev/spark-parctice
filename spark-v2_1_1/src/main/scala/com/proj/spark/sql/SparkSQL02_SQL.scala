package com.proj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_SQL {

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

    // DataFrame转换为一张表
    frame.createOrReplaceTempView("user")

    // 采用sql的语法访问数据
    spark.sql("select * from user").show()

    // 展示数据
//    frame.show()

    // 释放资源
    spark.stop()
  }
}
