package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/23 11:30
  */

// 输入输出样例类类型
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String )

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 用 ProcessFunction，实现2秒内连续登录失败的检测
    val warningStream = loginEventStream
      .keyBy( _.userId )    // 基于用户id进行分组
      .process( new LoginFailWarning(2) )

    warningStream.print()
    env.execute("login fail detect job")
  }
}

// 自定义process function，实现检测
class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义List状态变量，保存2秒之内的所有登录失败事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login fail list", classOf[LoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 判断当前数据是否是登录失败，如果是，保存到ListState，如果不是，清空状态和定时器
//    if( value.eventType == "fail" ){
//      loginFailListState.add(value)
//      // 如果没有注册定时器，注册一个2秒后的定时器
//      if( timerTsState.value() == 0 ){
//        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
//      }
//    } else {
//      loginFailListState.clear()
//      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
//      timerTsState.clear()
//    }

    // 分情况讨论：来的数据是成功还是失败；是否有已经保存到ListState里的登录失败
    if( value.eventType == "fail" ){
      // 1. 来的是失败，要继续判断ListState里是否有数据
      val iter = loginFailListState.get().iterator()
      if( iter.hasNext ){
        // 1.1 如果已经有登录失败，那么是连续两次登录失败，判断时间差
        val firstFail = iter.next()
        // 判断连续两次登录失败，时间戳差是否小于2秒，如果小于等于直接报警
        if( value.eventTime - firstFail.eventTime <= 2 )
          out.collect( Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2s.") )
        // 清理更新状态
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 如果之前没有登录失败，直接添加到ListState中
        loginFailListState.add(value)
      }
    } else {
      // 2.如果来的是成功，直接清空状态
      loginFailListState.clear()
    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    import scala.collection.JavaConversions._
//    val allLoginFailList = loginFailListState.get().toList
//
//    // 如果 fail 列表长度达到了定义的maxSize，那么输出报警
//    if( allLoginFailList.length >= maxFailTimes )
//      out.collect( Warning( allLoginFailList.head.userId,
//        allLoginFailList.head.eventTime,
//        allLoginFailList.last.eventTime,
//        "login fail in 2s for " + allLoginFailList.length + " times.") )
//
//    // 清空状态
//    loginFailListState.clear()
//    timerTsState.clear()
//  }
}