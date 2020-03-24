package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/24 9:22
  */

// 定义输入输出样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
case class OrderResult( orderId: Long, resultMsg: String )

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 0. 从文件中读取数据，按订单id分组
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 1. 定义一个 pattern，用来检测订单支付的复杂事件
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")    // 定义首先检测create事件
      .followedBy("pay").where(_.eventType == "pay")    // 定义后面跟着一个pay事件
      .within(Time.minutes(15))     // 时间限制在15分钟内

    // 2. 在输入数据流上应用pattern
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义一个侧输出流标签
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("order-timeout")

    // 4. 用select从pattern stream中提取匹配的事件
    val payedStream = patternStream.select( orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPySelect() )

    // 5. 从payedStream中提取侧输出流，得到订单超时事件
    val warningStream = payedStream.getSideOutput(orderTimeoutOutputTag)

    // 6. 打印输出
    payedStream.print("payed")
    warningStream.print("timeout")

    env.execute("order timeout job")
  }
}

// 自定义 PatternTimeoutFunction，用于处理检测到的超时事件
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("create").get(0).orderId
    OrderResult( timeoutOrderId, "timeout: " + timeoutTimestamp)
  }
}

// 自定义 PatternSelectFunction，用于处理检测到的成功支付事件
class OrderPySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult( payedOrderId, "payed successfully" )
  }
}
