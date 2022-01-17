package com.proj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {

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

    // 转换为DF
    val df: DataFrame = rdd.toDF("name", "age")

    // 转换为DS
    val ds: Dataset[User] = df.as[User]

    // 转换为DF
    val df1: DataFrame = ds.toDF()

    // 转换为RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row =>{
      // 获取数据时，可以通过索引访问数据
      println(row.mkString(","))
    })

    // 释放资源
    spark.stop()
  }
}

case class User(name: String, age: Int)