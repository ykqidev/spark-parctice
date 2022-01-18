package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Spark17_Json {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val json: RDD[String] = sc.textFile("in/json")

    val result: RDD[Option[Any]] = json.map(JSON.parseFull)

    result.foreach(println)

    sc.stop()


  }
}

