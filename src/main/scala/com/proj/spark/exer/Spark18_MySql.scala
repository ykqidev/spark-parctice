package com.proj.spark.exer

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_MySql {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://linux102:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    //    val sql = "select name,age from user where id >= ? and id <= ?";
    //    val jdbcRDD = new JdbcRDD(
    //      sc,
    //      () => {
    //        // 获取数据库数据对象
    //        Class.forName(driver)
    //        java.sql.DriverManager.getConnection(url, userName, passWd)
    //
    //      },
    //      sql,
    //      1,
    //      3,
    //      2,
    //      (rs) => {
    //        println(rs.getString(1) + "," + rs.getInt(2))
    //      }
    //    )
    //
    //    jdbcRDD.collect()

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("wangwu", 20), ("zhaoliu", 21)))

    //    dataRDD.foreach{
    //      case (str, i) => {
    //        Class.forName(driver)
    //        val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
    //
    //        val sql = "insert into user(name ,age) values(?,?)"
    //        val statement: PreparedStatement = connection.prepareStatement(sql)
    //        statement.setString(1,str)
    //        statement.setInt(2,i)
    //        statement.executeUpdate()
    //        statement.close()
    //        connection.close()
    //      }
    //    }


    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)

      datas.foreach {
        case (str, i) =>
          val sql = "insert into user(name ,age) values(?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, str)
          statement.setInt(2, i)
          statement.executeUpdate()
          statement.close()

      }
      connection.close()
    })


    sc.stop()


  }
}

