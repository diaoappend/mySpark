package com.diao.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: chenzhidiao
 * @date: 2020/8/16 18:14
 * @description:
 * @version: 1.0
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(conf,Seconds(10));

    val lines = ssc.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    words.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
