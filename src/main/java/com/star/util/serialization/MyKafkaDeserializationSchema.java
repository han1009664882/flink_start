package com.star.util.serialization;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author hanxingxing-001
 * @date 2021/1/15 14:10
 */
public class MyKafkaDeserializationSchema  implements KafkaDeserializationSchema<String> {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String value = new String(consumerRecord.value(), UTF_8.name());
        JSONObject jsonObject = JSONObject.parseObject(value);
        jsonObject.put("offset",consumerRecord.offset());
        jsonObject.put("partition",consumerRecord.partition());
//        jsonObject.put("dsg_key",consumerRecord.key());
        return jsonObject.toString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
