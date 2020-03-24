package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/24 10:36
  */
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 0. 从文件中读取数据，按订单id分组
    val resource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 自定义process function，实现主流输出正常匹配订单，侧输出流输出超时报警订单
    val payedStream = orderEventStream
      .process( new OrderPayMatch() )

    val warningStream = payedStream.getSideOutput(new OutputTag[OrderResult]("timeout"))

    // 打印输出
    payedStream.print("payed")
    warningStream.print("timeout")

    env.execute("order timeout without cep job")
  }
}

class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 定义状态，用来保存订单是否已create、是否已pay、定时器时间戳
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
  // 定义侧输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    // 判断当前来的订单事件类型，是create还是pay
    // 情况1：来的是create，判断是否pay过
    if( value.eventType == "create" ){
      // 1.1：已经pay过，支付匹配成功
      if( isPayed ){
        // 输出正常匹配结果到主流，清空状态，删除定时器
        out.collect( OrderResult( value.orderId, "payed successfully" ) )
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 1.2：如果还没有pay过，注册一个15分钟后的定时器，等待pay事件到来
      else {
        // 定义定时器时间戳，注册定时器，更新状态
        val ts = value.eventTime * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        isCreatedState.update(true)
        timerTsState.update(ts)
      }
    }
    // 情况2：来的是pay事件，判断是否create过
    else if( value.eventType == "pay" ){
      // 2.1：如果已经create过，要么匹配成功，要么pay已经超时
      if( isCreated ){
        // 2.1.1：如果没有超时，正常输出匹配结果
        if( value.eventTime * 1000L < timerTs )
          out.collect( OrderResult( value.orderId, "payed successfully" ) )
        // 2.1.2：如果已经超时，输出超时报警到侧输出流
        else
          ctx.output( orderTimeoutOutputTag, OrderResult( value.orderId, "payed but already timeout" ) )

        // 清空状态，删除定时器
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 2.2：如果没有create过，需要等待create
      else{
        // 注册定时器，等到pay事件发生的时间就可以了；另外更新状态
        val ts = value.eventTime * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        isPayedState.update(true)
        timerTsState.update(ts)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 如果定时器触发，一定是有一个事件没有到
    if( isPayedState.value() ){
      // 如果pay来过，说明create没到
      ctx.output(orderTimeoutOutputTag, OrderResult( ctx.getCurrentKey, "already payed but not found create log" ))
    } else{
      // 没有pay过，支付超时报警
      ctx.output(orderTimeoutOutputTag, OrderResult( ctx.getCurrentKey, "order pay timeout" ))
    }
  }
}