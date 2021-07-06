package com.diao.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: chenzhidiao
 * @date: 2020/8/16 18:14
 * @description:
 * @version: 1.0
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    //设置日志级别
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    //val sc = new SparkContext(conf)
    //val ssc1 = new StreamingContext(sc,Seconds(2))
    val ssc = new StreamingContext(conf,Seconds(3));

    val lines = ssc.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    words.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
