package com.star.flink.scala.apitest

import com.star.flink.scala.batch.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500L) //默认200ms
    //    env.setStateBackend(new MemoryStateBackend())
    env.setStateBackend(new FsStateBackend("checkpintDir"))
    env.enableCheckpointing(60000)
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //默认不延迟
      //      .assignAscendingTimestamps(_.timestamp)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })


    val processStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    //    状态编程的3中模式
    dataStream.keyBy(_.id)
      //      .process(new TempChangeAlert1(5))
      .flatMap(new TempChangeAlert2(10.0))

    dataStream.keyBy(_.id)
      //[输出类型，State类型]
      .flatMapWithState[(String, Double, Double), Double] {
        // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
        case (input: SensorReading, None) => (List.empty, Some(input.temperature))
        // 如果有状态的话，就应该与上次温度值比较差值，如果大于阈值就输出报警
        case (input: SensorReading, lastTemp: Some[Double]) => {
          val diff = (input.temperature - lastTemp.get).abs
          if (diff > 10.0)
            (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
          else (List.empty, Some(input.temperature))
        }
      }

    processStream.print()
    dataStream.print()
    env.execute("process function test")
  }
}

/**
 * 连续2s内，温度上升报警
 */
class TempIncreAlert extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  //定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()

    //更新温度值
    lastTemp.update(value.temperature)

    // currentTimer肯定有值
    val currentTimerTs = currentTimer.value()

    //温度上升且没有设过定时器，则注册定时器
    if (value.temperature > preTemp && currentTimerTs == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      //注意 registerProcessingTimeTimer 传入的是时间戳，而不是间隔
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    } else if (preTemp > value.temperature || preTemp == 0.0) {
      // 如果温度下降或是第一条数据，删除定时器，并清空状态
      ctx.timerService().deleteProcessingTimeTimer(currentTimerTs)
      currentTimer.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //直接输出报警信息
    out.collect(ctx.getCurrentKey + " 温度连续上升")
    currentTimer.clear()
  }
}

/**
 * 温度超过多少报警
 */
class TempChangeAlert1(threshold: Double)
  extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  // 定义一个状态变量， 保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    //获取上一次的温度值
    val lastTemp = lastTempState.value()
    //用当前温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }
}

class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //初试化的时候，声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //获取上一次的温度值
    val lastTemp = lastTempState.value()
    //用当前温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }
}
