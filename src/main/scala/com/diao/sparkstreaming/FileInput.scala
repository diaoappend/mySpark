package com.diao.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: chenzhidiao
 * @date: 2020/8/16 21:35
 * @description:
 * @version: 1.0
 */
object FileInput {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreamingfileinput")
    val ssc = new StreamingContext(conf,Seconds(2))


    val words: DStream[String] = ssc.textFileStream("D:\\MyWork\\mySpark\\input")

    val word = words.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    word.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
