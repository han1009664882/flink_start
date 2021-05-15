package com.star.flink.scala.apitest

import com.star.flink.scala.batch.SensorReading
import com.star.flink.scala.watermark.PeriodicAssigner
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500L) //默认200ms
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //默认不延迟
      //      .assignAscendingTimestamps(_.timestamp)
      .assignTimestampsAndWatermarks(new PeriodicAssigner)

    //统计10s内的最小温度
    val minTempStream = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //第三个参数代表时区
      //      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))
      .timeWindow(Time.seconds(10)) //开时间窗口，窗口时间左闭右开
      //窗口时间10s，滑动窗口5s
      //      .timeWindow(Time.seconds(10),Time.seconds(5))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))


    minTempStream.print()
    dataStream.print()
    env.execute("window test")
  }
}

