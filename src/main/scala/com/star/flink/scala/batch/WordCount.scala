package com.star.flink.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    //    val port = ParameterTool.fromArgs(args).getInt("port")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("E:/workspace/flink_start/data")
    val count = text.map{x=>(x.split(",")(0),1)}
      .groupBy(0)
      .sum(1)

    count.print()
  }
}
