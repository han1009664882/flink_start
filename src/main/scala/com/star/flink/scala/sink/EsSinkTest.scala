package com.star.flink.scala.sink

import java.util

import com.star.flink.scala.batch.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.client.{Requests, RestClientBuilder}

object EsSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamFromFile = env.readTextFile("E:\\workspace\\flink_start\\src\\main\\resources\\sensor.txt")
    val dataStream = streamFromFile.map(line => {
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    //创建一个es sink builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts, new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, runtimeContext: RuntimeContext, indexer: RequestIndexer): Unit = {
          println("data save")
          //包装成map或者jsonObject
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", element.id);
          json.put("temperature", element.temperature.toString)
          json.put("timestamp", element.timestamp.toString)

          //创建index request
          val indexRequest = Requests.indexRequest().index("sensor")
            .`type`("readingdata")
            .source(json)
          //利用index发送数据
          indexer.add(indexRequest)
        }
      })

    esSinkBuilder.setBulkFlushMaxActions(1)
//    esSinkBuilder.setRestClientFactory(new RestClientFactory() {
//      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
//        restClientBuilder.setDefaultHeaders(...)
//        restClientBuilder.setMaxRetryTimeoutMillis(...)
//        restClientBuilder.setPathPrefix(...)
//        restClientBuilder.setHttpClientConfigCallback(...)
//
//      }
//    })

    dataStream.addSink(esSinkBuilder.build())
    env.execute("es sink")
  }
}
