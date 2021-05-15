package com.star.flink.realtime;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.star.flink.constant.Constants;
import com.star.util.serialization.MyKafkaDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final Joiner JOINER = Joiner.on(" || ").useForNull("");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
//        env.disableOperatorChaining(); //不使用处理链

        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.PRODUCT_SERVERS);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.TEST_BASIC2_SERVERS);
        properties.setProperty("group.id", "dmgr_check");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("jy_dsg_test01", new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("TMP_SYS0022", new MyKafkaDeserializationSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> mapStream = env.addSource(consumer).map(value -> {
            JSONObject json = JSONObject.parseObject(value);
            int partition = json.getIntValue("partition");
            long offset = json.getLongValue("offset");
            String rowid = json.getString("rowid");
            String tableName = json.getString("tableName");
//            String dsgKey = json.getString("dsg_key");
            return JOINER.join( partition, offset, rowid, tableName, value);
        });
//                .startNewChain();
//                .disableChaining();


        mapStream.addSink(StreamingFileSink.forRowFormat(new Path("E:\\sys0002\\tmp"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.create()
                        // 设置每个文件的最大大小 ,默认是128M。这里设置为120M
                        .withMaxPartSize(1024 * 1024 * 120)
                        // 滚动写入新文件的时间，默认60s。这里设置为无限大
                        .withRolloverInterval(Long.MAX_VALUE)
                        // 10分钟空闲，就滚动写入新的文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))
                        .build())
                .build());
        env.execute("test");
    }
}
