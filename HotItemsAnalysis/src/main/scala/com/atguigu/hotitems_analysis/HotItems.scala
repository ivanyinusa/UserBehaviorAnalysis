package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/20 11:40
  */

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义中间聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 创建流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度，为了测试显示方便设成1
    env.setParallelism(1)

    // 从文件中读取数据，并map成样例类类型，指定时间戳和生成watermark
    //    val dataStream: DataStream[UserBehavior] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 更换kafka数据源
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream = env.addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义转换聚合操作
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 1. 过滤出pv行为，作为热门度的统计指标
      .keyBy(_.itemId) // 2. 按照itemId做一个分组
      .timeWindow(Time.hours(1), Time.minutes(5)) // 3.开滑动窗口进行聚合统计
      .aggregate(new CountAgg(), new CountWindowResult())

    // 定义排序输出操作
    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd) // 4. 对聚合结果按照窗口分组
      .process(new TopNHotItems(5)) // 5. 对当前窗口所有count数据排序输出

    // 控制台打印输出
    resultStream.print()
    env.execute("hot items job")
  }
}

// 自定义一个预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  // 每来一条数据的时候，状态怎么改变，count + 1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，结合聚合的count值，以及当前窗口的信息，输出样例类
class CountWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(key, windowEnd, count))
  }
}

// 自定义的 process function，用于将count结果排序输出，并生成格式化String数据
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 定义一个列表状态，用来保存当前窗口所有商品的count值
  lazy val itemListState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 所有来的数据，全部保存到ListState里
    itemListState.add(value)
    // 注册一个 windowEnd+1 触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，数据全部到齐，可以进行排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 把列表状态中的数据都取出来，放到一个List里
    import scala.collection.JavaConversions._
    val allItemsList: List[ItemViewCount] = itemListState.get().toList

    // 清空状态
    itemListState.clear()

    // 按照count大小排序，取前topSize个
    val sortedItemsList: List[ItemViewCount] = allItemsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排名信息格式化成String，打印输出
    val result: StringBuilder = new StringBuilder()
    // 当前窗口信息
    result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 取出排序列表中的数据，依次输出
    for (i <- sortedItemsList.indices) {
      val currentItemCount = sortedItemsList(i)
      result.append("NO").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItemCount.itemId)
        .append(" 浏览量=").append(currentItemCount.count)
        .append("\n")
    }
    result.append("--------------------------------\n")
    // 控制输出频率
    Thread.sleep(1000L)
    out.collect(result.toString())
  }
}

// 扩展： 用 AggregateFunction实现求平均数的需求
class AverageTsAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double]{
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
    (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2.toDouble

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}