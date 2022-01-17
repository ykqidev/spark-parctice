package com.proj.spark.exer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Var {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))

//    val rdd2 = sc.makeRDD(List((1,1),(2,2),(3,3)))
    /*
      (1,(a,1)),(2,(b,2)),(3,(c,3))
     */
//    val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
//    println(joinRDD.collect().mkString(","))

    val list = List((1,1),(2,2),(3,3))

    // 可以使用广播变量减少数据的传播
    // 1.构建广播变量
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) =>
        var v2: Any = null
        // 2.使用广播变量
        for (elem <- broadcast.value) {
          if (key == elem._1) {
            v2 = elem._2
          }
        }
        (key, (value, v2))
    }

    resultRDD.foreach(println)

    sc.stop()


  }
}

