package com.atguigu.newworkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/21 9:18
  */

// 输入日志数据的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 中间聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HotPageNetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val dataStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(" ")
        // 对日志中的日期时间做转换，得到时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
      })
      // 指定时间戳和生成watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })

    // 按照url分组，开滑动窗口进行聚合
    val aggStream = dataStream
      .keyBy( _.url )
      .timeWindow( Time.minutes(10), Time.seconds(10) )
        .allowedLateness( Time.minutes(1) )
        .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate( new UrlCountAgg(), new UrlCountResult() )

    // 聚合结果排序输出
    val resultStream = aggStream
      .keyBy( _.windowEnd )
      .process( new TopNHotPages(3) )

    val lateStream = aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late"))
    // 结果打印输出
    resultStream.print()
    dataStream.print("input")
    aggStream.print("agg")
    lateStream.print("late")
    env.execute()
  }
}

// 自定义一个预聚合函数
class UrlCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
// 自定义一个窗口函数
class UrlCountResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect( PageViewCount(key, window.getEnd, input.iterator.next()) )
  }
}

// 自定义process function做排序输出
class TopNHotPages(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{
  // 定义一个Map状态，保存当前窗口所有的count聚合结果，（url，count）
  private var pageMapState: MapState[String, Long] = _

  override def open(parameters: Configuration): Unit = {
    pageMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("page-map", classOf[String], classOf[Long]))
  }

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 先把所有聚合结果从状态中提取出来
    val allPagesCountList: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    val iter = pageMapState.entries().iterator()
    while( iter.hasNext ){
      val entry = iter.next()
      allPagesCountList += ((entry.getKey, entry.getValue))
    }

//    pageMapState.clear()

    // 排序截取TopN
    val sortedPagesList = allPagesCountList.sortWith(_._2 > _._2).take(topSize)

    // 格式化String输出
    val result: StringBuilder = new StringBuilder()

    result.append("窗口关闭时间：").append( new Timestamp(timestamp-1) ).append("\n")
    for (i <- sortedPagesList.indices) {
      val currentUrlView: (String, Long) = sortedPagesList(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView._1)
        .append("  流量=").append(currentUrlView._2).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)

    out.collect(result.toString())
  }
}