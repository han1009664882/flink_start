package com.star.flink.realtime;

import com.google.common.base.Joiner;
import com.star.flink.constant.Constants;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaJsonConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonConsumer.class);
    private static final Joiner JOINER = Joiner.on(" || ").useForNull("");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.PRODUCT_SERVERS);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.TEST_BASIC2_SERVERS);
        properties.setProperty("group.id", "dmgr_check");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // json会有隐藏字符问题
        FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>("TMP_SYS0022", new JSONKeyValueDeserializationSchema(true), properties);
        consumer.setStartFromEarliest();
        DataStream<String> mapStream = env.addSource(consumer).map(value -> {
            JsonNode metadata = value.get("metadata");
            int partition = metadata.get("partition").asInt();
            long offset = metadata.get("offset").asLong();
//            JsonNode jsonValue = value.get("value");
//            String rowid = jsonValue.get("rowid").asText("");
//            String tableName = jsonValue.get("tableName").asText("");
//            return JOINER.join(partition, offset, rowid, tableName, jsonValue.toString());
//            return JOINER.join(partition, offset, jsonValue.toString());
            return JOINER.join(partition,offset,value.get("value"));
        });
        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(new Path("E:\\sys0002\\tmp"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.create()
                        // 设置每个文件的最大大小 ,默认是128M。这里设置为120M
                        .withMaxPartSize(1024 * 1024 * 120)
                        // 滚动写入新文件的时间，默认60s。这里设置为无限大
                        .withRolloverInterval(Long.MAX_VALUE)
                        // 10分钟空闲，就滚动写入新的文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))
                        .build())
                .build();

        mapStream.addSink(fileSink);
        env.execute("test");
    }
}
