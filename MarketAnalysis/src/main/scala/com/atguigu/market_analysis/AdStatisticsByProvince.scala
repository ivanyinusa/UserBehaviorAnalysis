package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/23 10:12
  */

// 定义输入输出样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
case class AdViewCountByProvince( windowEnd: String, province: String, count: Long )
// 定义侧输出流的报警信息样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String)

object AdStatisticsByProvince {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        AdClickEvent( dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 用process function进行黑名单过滤，并将黑名单输出到侧输出流
    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream
      .keyBy( data => (data.userId, data.adId) )    // 按照用户和广告Id进行分组
      .process( new FilterBlackListUser(100) )    // 对点击行为进行计数，如果超过上限就报警过滤

    // 开窗聚合
    val adCountStream: DataStream[AdViewCountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountResult() )

    adCountStream.print("adCount")
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("blacklist")
    env.execute()
  }
}

// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class AdCountResult() extends WindowFunction[Long, AdViewCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCountByProvince]): Unit = {
    val endTime = new Timestamp( window.getEnd ).toString
    out.collect( AdViewCountByProvince(endTime, key, input.iterator.next()) )
  }
}

// 自定义ProcessFunction，将用户对某个广告的点击量保存成状态，如果超过上限就输出报警，并设置0点定时器每天清空
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
  // 定义状态，保存当前用户对当前广告的点击量，用户是否已经发送到侧输出流黑名单，0点重置状态的定时器时间戳
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent", classOf[Boolean]))
  lazy val resetTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer", classOf[Long]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    // 从状态中取出 count值
    val count = countState.value()

    // 如果是第一次点击数据，注册一个0点定时器
    if( count == 0 ){
      // 获取第二天0点的时间戳
      val ts = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000)
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTimerState.update(ts)
    }

    // 判断，如果count达到上限，那么加入黑名单，侧输出流输出报警，主流里过滤掉
    if( count >= maxCount ){
      // 判断是否已经输出过黑名单报警，只输出一次
      if( !isSentState.value() ){
        // 如果没有输出过，侧输出流输出黑名单报警
        ctx.output(new OutputTag[BlackListWarning]("blacklist"),
          BlackListWarning(value.userId, value.adId, "click over " + maxCount + "times today."))
        // 更新状态
        isSentState.update(true)
      }
      return
    }
    // count加1，并将数据输出到主流
    countState.update( count + 1 )
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    // 判断如果确实是0点的时间，那么清空状态
    if( timestamp == resetTimerState.value() ){
      isSentState.clear()
      countState.clear()
      resetTimerState.clear()
    }
  }
}