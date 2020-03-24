package com.atguigu.orderpay_detect

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/24 15:43
  */
object OrderTxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件中读取数据，只取pay事件，按交易id分组
    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderResource.getPath)
//    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)

    // 从文件中读取数据，按交易id分组
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
//    val receiptEventStream = env.socketTextStream("localhost", 7788)
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent( dataArray(0), dataArray(1), dataArray(2).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 将两条流做 join
    val processedStream = orderEventStream.intervalJoin(receiptEventStream)
      .between( Time.seconds(-3), Time.seconds(5) )
      .process( new OrderPayTxMatchByJoin() )

    // window join示例
    val processedStream2 = orderEventStream.join(receiptEventStream)
      .where(_.txId)
      .equalTo(_.txId)
      .window( TumblingEventTimeWindows.of(Time.minutes(1)) )
      .apply( new MyJoinFunc() )

    processedStream.print("matched")

    env.execute("order tx match job")
  }
}

// 自定义实现一个ProcessJoinFunction
class OrderPayTxMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect( (left, right) )
  }
}

class MyJoinFunc() extends JoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def join(first: OrderEvent, second: ReceiptEvent): (OrderEvent, ReceiptEvent) = {
    (first, second)
  }
}