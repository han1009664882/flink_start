package com.star.flink.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDataStream = env.socketTextStream("localhost",7777)
    val resultDataStream=socketDataStream.flatMap(_.split(" "))
      .filter(_.length>2)
      .map((_,1))
      .keyBy(0)
      .sum(1)
    resultDataStream.print()
    env.execute("Stream word count")

  }
}
