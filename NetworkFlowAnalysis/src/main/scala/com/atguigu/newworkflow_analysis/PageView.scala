package com.atguigu.newworkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/21 11:38
  */

// 定义输入输出数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度，为了测试显示方便设成1
    env.setParallelism(1)

    // 从文件中读取数据，并map成样例类类型，指定时间戳和生成watermark
    val resouce = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = env.readTextFile(resouce.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义转换操作，提取pv行为，开窗聚合
    val resultStream: DataStream[PvCount] = dataStream
      .filter( _.behavior == "pv" )    // 1. 筛选 pv行为
      .map( data => ("pv", 1) )    // 2. map成一个二元组，类似 word count
//    .map( data => {
//      val key = Random.nextInt(Int.MaxValue).toString
//      (key, 1)
//    } )
      .keyBy( _._1 )     // 3. 按照 dummy key进行分组，全部分到一个组内聚合
      .timeWindow( Time.hours(1) )    // 4. 开窗，滚动窗口
      .aggregate( new PvCountAgg(), new PvCountResult() )    // 5. 聚合统计

//      .keyBy(_.windowEnd)
//      .process( new TotalCountProcess() )

    resultStream.print()
    env.execute()
  }
}

// 实现自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Int), Long, Long]{
  override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 实现自定义的窗口函数
class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect( PvCount(window.getEnd, input.iterator.next()) )
  }
}