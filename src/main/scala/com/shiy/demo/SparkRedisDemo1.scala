package com.shiy.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by DebugSy on 2019/3/13.
  */
object SparkRedisDemo1 {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .config("spark.redis.host", "192.168.1.83")
      .config("spark.redis.port", "6379")
      .master("local[*]")
      .appName("SparkRedis")
      .getOrCreate()

    val studentDF = sc.read.option("header", "true").csv("src/main/resources/students.txt")
    studentDF.show()

    studentDF.write
      .format("org.apache.spark.sql.redis")
      .option("table", "shiy_student_cache")
      .option("key.column", "sId")
      .save()


  }

}
