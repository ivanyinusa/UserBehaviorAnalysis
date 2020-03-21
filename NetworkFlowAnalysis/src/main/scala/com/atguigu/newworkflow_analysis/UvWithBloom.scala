package com.atguigu.newworkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.util.hashing.MurmurHash3

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/21 14:53
  */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度，为了测试显示方便设成1
    env.setParallelism(1)

    // 从文件中读取数据，并map成样例类类型，指定时间戳和生成watermark
    val resouce = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = env.readTextFile(resouce.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义转换操作，提取pv行为，开窗聚合
    val resultStream: DataStream[UvCount] = dataStream
      .filter( _.behavior == "pv" )
      .map( data => ("dummyKey", data.userId) )
      .keyBy( _._1 )
      .timeWindow( Time.hours(1) )
        .trigger( new MyTrigger() )    // 自定义窗口触发规则
      .process( new UvCountWithBloom() )    // 自定义窗口计算规则

    resultStream.print()
    env.execute()
  }
}

// 自定义Trigger，每来一条数据，就触发一次窗口计算
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  // 每来一条数据，就触发窗口计算，并且清空窗口状态
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
    TriggerResult.FIRE_AND_PURGE
}

// 定义一个布隆过滤器类，位图大小从外部传入
class Bloom(size: Long) extends Serializable{
  // 内部属性，位图的大小，必须是 2^N
  private val cap = size

  // 定义hash函数，用于将一个String转化成一个offset
  def hash( value: String, seed: Int ) : Long ={
    var result = 0L
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }
    // 返回offset值，需要让它在cap范围内
    (cap - 1) & result
  }
}

// 自定义process function，相当于每条数据来了都会调用
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  // 定义redis连接，创建布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  // 位图大小为约 10^9，由于需要2的整次幂，所以转换成了 2^30
  lazy val bloom = new Bloom(1<<30)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 存储方式：一个窗口对应一个位图，所以以windowEnd为 key，(windowEnd, bitmap)
    val storeKey = context.window.getEnd.toString
    // 简单起见，把当前窗口的 UV count值也写入到 redis中，作为状态保存起来，(windowEnd, count)
    val hashmapName = "UVcount"
    var count = 0L
    // 从redis UVcount表读取当前窗口的count值
    if( jedis.hget(hashmapName, storeKey) != null )
      count = jedis.hget("UVcount", storeKey).toLong

    // 判断当前userId 是否出现在 bitmap中
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)    // 求当前 userId的hash值，就是位图中的offset
    val isExist = jedis.getbit( storeKey, offset )    // 查看位图位置为止是否为1
    if( !isExist ){
      // 如果不存在，对应位置置1，count + 1
      jedis.setbit( storeKey, offset, true )
      jedis.hset( hashmapName, storeKey, (count + 1).toString )
      // 输出 UVcount
      out.collect( UvCount(storeKey.toLong, count + 1) )
    }
  }
}
