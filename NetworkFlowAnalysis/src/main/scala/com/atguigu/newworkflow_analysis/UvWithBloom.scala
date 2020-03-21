package com.atguigu.newworkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/21 14:53
  */
object UvWithBloom {
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
      .filter( _.behavior == "pv" )
      .map( data => ("dummyKey", data.userId) )
      .keyBy( _._1 )
      .timeWindow( Time.hours(1) )
      .process( new UvCountWithBloom() )

    resultStream.print()
    env.execute()
  }
}
