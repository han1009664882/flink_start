package com.star.flink.scala.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.star.flink.scala.batch.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // flink1.11开始有提供jdbc sink
    dataStream.addSink(new MyJdbcSink)
    env.execute("jdbc sink")
  }

}

class MyJdbcSink extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "user", "pwd")
    insertStatement = conn.prepareStatement("insert into table1 ( sensor,temp) values(?,?)")
    updateStatement = conn.prepareStatement("update table1 set temp=? where sensor=?")
  }

  override def close(): Unit = {
    super.close()
    insertStatement.close()
    updateStatement.close()
    conn.close()
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStatement.setDouble(1, value.temperature)
    updateStatement.setString(2, value.id)
    updateStatement.execute()
    //没查到，插入
    if (updateStatement.getUpdateCount == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setDouble(2, value.temperature)
      insertStatement.execute()
    }
  }
}
