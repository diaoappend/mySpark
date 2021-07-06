package com.diao.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: chenzhidiao
 * @date: 2020/9/27 9:42
 * @description:
 * @version: 1.0
 */
object UpdateStateBykeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("updateStateBykey").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("d:/checkpoint")

  }

}
