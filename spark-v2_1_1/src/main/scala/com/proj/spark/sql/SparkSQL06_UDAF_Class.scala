package com.proj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL06_UDAF_Class {

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

    // 自定义聚合函数
    // 创建聚合函数对象
    val udaf = new MyAgeAvgClassFunction
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    // 使用聚合函数
    val frame: DataFrame = spark.read.json("in/json/user.json")

    val userDS: Dataset[UserBean] = frame.as[UserBean]

    // 应用函数
    userDS.select(avgCol).show()

    // 释放资源
    spark.stop()
  }
}

case class UserBean(name: String, age: Long)

case class AvgBugger(var sum: Long, var count: Int)

// 声明用户自定义聚合函数
// 1) 继承Aggregator，设定泛型
// 2) 实现方法
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBugger, Double] {
  // 初始化
  override def zero: AvgBugger = {
    AvgBugger(0, 0)
  }

  // 聚合数据
  override def reduce(b: AvgBugger, a: UserBean): AvgBugger = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  override def merge(b1: AvgBugger, b2: AvgBugger): AvgBugger = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: AvgBugger): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBugger] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}