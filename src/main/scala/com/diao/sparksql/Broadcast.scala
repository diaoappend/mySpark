package com.diao.sparksql

import org.apache.spark.sql.SparkSession

/**
 * @author: chenzhidiao
 * @date: 2021/6/18 10:50
 * @description:
 * @version: 1.0
 */
object Broadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Broadcast").master("local").getOrCreate()

    import spark.implicits._

    val in = spark.sparkContext.textFile("D:\\MyWork\\mySpark\\input\\employee.txt")

    val df = in.toDF("id","name","age")



  }

}

case class employee(id:Int,name:String,age:Int)