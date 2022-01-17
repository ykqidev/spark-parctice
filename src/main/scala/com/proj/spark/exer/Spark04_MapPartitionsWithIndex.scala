package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val tupA: (String, String) = ("Good", "Morning!")
    val tupB: (String, String) = ("Guten", "Tag!")
    for (tup <- List(tupA, tupB)) {
      tup match {
        case (thingOne, thingTwo) if thingOne == "Good" =>
          println("A two-tuple starting with 'Good'.")
        case (thingOne, thingTwo) => println("This has two things: " + thingOne + " and " + thingTwo)
      }
    }

    println("===============================================================")
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, data) =>
        data.map((_, "分区号：" + num))
    }

    println(tupleRDD.collect().mkString(","))


  }
}
