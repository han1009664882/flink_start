package com.star.flink.scala.batch

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从自定义集合读取数据
    val stream1 = env.fromCollection(List(SensorReading("sensor_1", 154778199, 35.000123),
      SensorReading("sensor_2", 154779199, 35.000123),
      SensorReading("sensor_3", 154788199, 35.000123)))

    // 2. 从文件读取
    val stream2 = env.readTextFile("E:/workspace/flink_start/data")


    // 3. 自定义source
    val stream3 = env.addSource(new SensorSource())

    stream3.print("stream1")
    env.execute("source test")
  }
}

class SensorSource() extends SourceFunction[SensorReading] {
  var running = true

  var count:Int =0

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    var curTemp = 1.to(10).map { i => ("sensor_" + i, 60 + rand.nextGaussian() * 20) }

    while (running) {
      curTemp = curTemp.map(t => (t._1, t._2 + rand.nextGaussian()))
      val curTime = System.currentTimeMillis()
      curTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))
      count+=1
      // 20 * 10 条数据
      if(count ==20){
        running = false
      }
    }
    Thread.sleep(500)

  }

  // 取消数据源生成
  override def cancel(): Unit = {
    running = false
  }
}
