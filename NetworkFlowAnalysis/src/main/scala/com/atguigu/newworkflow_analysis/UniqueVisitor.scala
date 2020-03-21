package com.atguigu.newworkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/21 14:20
  */

case class UvCount( windowEnd: Long, count: Long )

object UniqueVisitor {
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
    val resultStream: DataStream[UvCount] = dataStream
      .filter( _.behavior == "pv" )    // 1. 筛选 pv行为
      .timeWindowAll( Time.hours(1) )    // 2. 开窗，滚动窗口
      .apply( new UvCountByWindow() )    // 3. 自定义窗口函数聚合统计

    resultStream.print()
    env.execute()
  }
}

// 实现自定义的窗口函数，把所有数据放在set里，进行去重
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var userIdSet = Set[Long]()
    // 遍历窗口中的所有数据，把userId添加到set里，直接去重
    for( userBehavior <- input )
      userIdSet += userBehavior.userId
    // 输出userId的数量
    out.collect( UvCount(window.getEnd, userIdSet.size) )
  }
}