package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/23 15:51
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 0. 从文件中读取数据，并按 userId分组
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)

    // 1. 定义一个 pattern，2秒之内连续登录失败
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")   // 以一个登录失败事件开始
      .next("secondFail").where(_.eventType == "fail")    // 紧跟一个登录失败事件
//        .next("thirdFail").where(_.eventType == "fail")
      .within(Time.seconds(2))    // 在两秒内有效

    // 2. 在输入的数据流上应用pattern，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    // 3. 从 PatternStream中检出匹配的事件流，定义转换操作
    val warningStream: DataStream[Warning] = patternStream.select( new LoginFailMatch() )

    warningStream.print()
    env.execute("login fail detect job")
  }
}

// 自定义一个 PatternSelectFunction
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照对应的key，提取匹配的事件
    val firstFailEvent = map.get("firstFail").get(0)
    val lastFailEvent = map.get("secondFail").get(0)
    Warning( firstFailEvent.userId, firstFailEvent.eventTime, lastFailEvent.eventTime, "login fail for 2 times." )
  }
}
