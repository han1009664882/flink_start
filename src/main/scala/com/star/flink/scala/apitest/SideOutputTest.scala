package com.star.flink.scala.apitest

import com.star.flink.scala.batch.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object SideOutputTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500L) //默认200ms
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }) //默认不延迟
      //      .assignAscendingTimestamps(_.timestamp)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

   val processStream = dataStream.process(new FreezingAlert())

    //打印侧输出流
    processStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")
    env.execute("side output test")
  }
}

/**
 * 冰点报警，如果小于32F，输出报警信息到侧输出流
 */
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32) {
      ctx.output(alertOutput, "freezing alert for " + value.id)
    } else {
      out.collect(value)
    }
  }
}
