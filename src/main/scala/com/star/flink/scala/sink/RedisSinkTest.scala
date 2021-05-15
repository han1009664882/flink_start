package com.star.flink.scala.sink

import com.star.flink.scala.batch.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val conf = new FlinkJedisPoolConfig.Builder().setHost("").setPort(6379).build()
    dataStream.addSink(new RedisSink(conf,new MyRedisMapper))
    env.execute("redis sink")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
  /**
   * 定义保存数据到redis的命令
   *
   * @return
   */
  override def getCommandDescription: RedisCommandDescription = {
    //保存hash表 hset(key,field,value)
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")

  }

  /**
   * 定义保存到redis的key
   */
  override def getKeyFromData(data: SensorReading): String = data.id

  /**
   * 定义保存到redis的value
   */
  override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
