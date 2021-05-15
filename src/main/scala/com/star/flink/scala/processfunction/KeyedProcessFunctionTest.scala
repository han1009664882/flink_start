package com.star.flink.scala.processfunction

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 *
 */
class KeyedProcessFunctionTest[KEY, IN, OUT] extends KeyedProcessFunction[KEY, IN, OUT] {

  /**
   * 流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出。
   * Context可以访问元素的时间戳、Key以及TimerServer时间服务。
   * Context还可以将结果输出到别的流(side outputs)
   *
   * @param value
   * @param ctx
   * @param out
   */
  override def processElement(value: IN, ctx: KeyedProcessFunction[KEY, IN, OUT]#Context, out: Collector[OUT]): Unit = ???

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[KEY, IN, OUT]#OnTimerContext, out: Collector[OUT]): Unit = super.onTimer(timestamp, ctx, out)
}
