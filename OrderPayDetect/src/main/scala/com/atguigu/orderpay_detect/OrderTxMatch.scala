package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/24 11:50
  */

// 定义收据流输入数据的样例类
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object OrderTxMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件中读取数据，只取pay事件，按交易id分组
    val orderResource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(orderResource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)

    // 从文件中读取数据，按交易id分组
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
//    val receiptEventStream = env.readTextFile(receiptResource.getPath)
    val receiptEventStream = env.socketTextStream("localhost", 7788)
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent( dataArray(0), dataArray(1), dataArray(2).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 将两条流做合并，然后用process function进行处理，并将不匹配的事件输出到侧输出流
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process( new OrderPayTxMatch() )

    // 通过侧输出流标签提取不匹配事件
    val unmatchedPayStream = processedStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pays"))
    val unmatchedReceiptStream = processedStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipts"))

    processedStream.print("matched")
    unmatchedPayStream.print("unmatched-pays")
    unmatchedReceiptStream.print("unmatched-receipts")
    env.execute("order tx match job")
  }
}

// 实现自定义的CoProcessFunction，分别判断处理两条流的数据，把另一条流对应的数据存成状态
class OrderPayTxMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 定义状态，分别用来保存当前交易tx对应的pay事件和receipt事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))
  // 定义侧输出流标签
  val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")


  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单的支付事件pay来了，需要判断对应的receipt是否来过
    val receipt = receiptState.value()
    if( receipt != null ){
      // 如果已经到账，那么正常匹配，输出主流信息
      out.collect( (pay, receipt) )
      receiptState.clear()
    } else{
      // 如果还没到账，那么把pay存入状态，注册定时器等待
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )    // 等待5秒钟
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单的到账事件receipt来了，需要判断对应的pay是否来过
    val pay = payState.value()
    if( pay != null ){
      // 如果已经有pay事件，那么正常匹配，输出主流信息
      out.collect( (pay, receipt) )
      payState.clear()
    } else{
      // 如果pay还没来，那么把 receipt存入状态，注册定时器等待
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 3000L )    // 等待3秒钟
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 定时器触发，要判断到底是哪个数据没有来
    if( payState.value() != null ){
      // receipt没来
      ctx.output(unmatchedPays, payState.value())
    }
    if( receiptState.value() != null ){
      // pay没来
      ctx.output(unmatchedReceipts, receiptState.value())
    }
    // 状态清理
    payState.clear()
    receiptState.clear()
  }
}