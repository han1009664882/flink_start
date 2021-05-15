package com.star.flink.scala.batch

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    //    dataStream.keyBy(0).sum(2).print()
    //    dataStream.keyBy("id").sum("temperature").print()

    // 输出当前传感器最新的温度+10.时间是上一次数据的时间+1
    //    dataStream.keyBy("id")
    //      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
    //      .print()

    val splitStream = dataStream.split(data => if (data.temperature > 30) Seq("high") else Seq("low"))
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    val warning = high.map(data => (data.id, data.temperature))


    //connect只能合并两个流;union可以合并多个，但数据结构必须一样
//    val coMapStream = warning.connect(low)
//      .map(warningData => (warningData._1, warningData._2, "warning"),
//        lowData => (lowData.id, "healthy"))
    //    high.print("high")
    //    low.print("low")
    //    all.print("all")
//    coMapStream.print()

    env.execute("transform test")
  }
}

class MyFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = value.id.startsWith("sensor_1")
}

class MyRichMapper() extends RichMapFunction[SensorReading,String]{

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.getIndexOfThisSubtask
    super.open(parameters)
  }

  override def map(value: SensorReading): String = {
    value.id
  }
}
