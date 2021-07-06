package com.diao.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * @author: chenzhidiao
 * @date: 2021/6/21 9:02
 * @description:
 * @version: 1.0
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[2]")
      .appName("hive-demo")
//      .enableHiveSupport()
      .getOrCreate()
    import session.implicits._
    session.sparkContext.setLogLevel("error")

    session.conf.set("spark.sql.autoBroadcastJoinThreshold",1000000)


    //session.sql("show databases")
//    session.sql("use video")
//    val frame = session.sql(
//      """
//        |select
//        |    t2.videoId,
//        |    t2.views,
//        |    t2.ratings,
//        |    t1.videos,
//        |    t1.friends
//        |from (
//        |    select
//        |        *
//        |    from
//        |        gulivideo_user_orc
//        |    order by
//        |        videos desc
//        |    limit
//        |        10) t1
//        |join
//        |    gulivideo_orc t2
//        |on
//        |    t1.uploader = t2.uploader
//        |order by
//        |    views desc
//        |limit
//        |    20
//        |""".stripMargin)
//
//    frame.show(100)
//
//    Thread.sleep(1000000)

    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("age", IntegerType)


    val df = session.read.format("json").json("D:\\MyWork\\mySpark\\input\\demo.json")
    df.printSchema()
    df.show()

  }

}
