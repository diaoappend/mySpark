package com.diao.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: chenzhidiao
 * @date: 2021/6/20 10:39
 * @description:
 * @version: 1.0
 */
object Scheduler {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("scheduler").setMaster("local[*]")
    //在conf配置文件对象中设置调度策略
    conf.set("spark.scheduler.mode","FAIR")
    val sc = new SparkContext(conf)
    //在SparkContext对象中设置调度的队列
    sc.setLocalProperty("spark.scheduler.pool","test")

  }

}
