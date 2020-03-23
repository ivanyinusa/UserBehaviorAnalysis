package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/23 9:11
  */

// 定义输入的样例类类型
case class MarketingUserBehavior( userId: String, behavior: String, channel: String, timestamp: Long )

// 输出结果样例类
case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )

// 自定义一个测试的数据源
class SimulatedEventSource() extends RichParallelSourceFunction[MarketingUserBehavior]{
  // 定义是否运行的标识位
  var running = true
  // 定义随机生成行为和渠道的集合
  val behaviorSet = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSet = Seq("appstore", "weibo", "wechat")
  // 定义一个随机数生成器
  val rand = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个最大生成的数据量
    val maxElements = Long.MaxValue
    var count = 0L

    // 无限循环，随机生成数据
    while( running && count < maxElements ){
      // 数据所有字段随机生成（除ts）
      val userId = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      // 包装成样例类输出
      ctx.collect( MarketingUserBehavior(userId, behavior, channel, ts) )

      // 计数更新
      count += 1
      Thread.sleep(10L)
    }
  }
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源
    val dataStream = env.addSource( new SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)

    // 转换开窗聚合
    val aggStream = dataStream
      .filter( _.behavior != "UNINSTALL" )    // 过滤掉卸载行为
      .keyBy( data => (data.channel, data.behavior) )    // 基于两个key做分组
      .timeWindow( Time.hours(1), Time.seconds(1) )    // 开滑动窗口做聚合
      .process( new MarketingCountByChannel() )

    aggStream.print()
    env.execute("app marking job")
  }
}

// 自定义的ProcessWindowFunction
class MarketingCountByChannel() extends ProcessWindowFunction[MarketingUserBehavior, MarketingViewCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingViewCount]): Unit = {
    val startTime = new Timestamp(context.window.getStart).toString
    val endTime = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect( MarketingViewCount(startTime, endTime, channel, behavior, count) )
  }
}