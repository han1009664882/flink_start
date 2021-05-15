package com.star.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

public class FlinkStreamJavaExample {

    public static void main(String[] args) {
        //./bprintln(in/flink run
        // --class com.hxx.flink.FlinkStreamJavaExample /opt/test.jar
        // --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2
        // 读取文本路径信息，并使用逗号分隔
        String[] filePaths = ParameterTool.fromArgs(args)
                .get("filePath","log1.txt,log2.txt")
                .split(",");
        assert filePaths.length > 0;
        String windowTime = ParameterTool.fromArgs(args).get("windowTime", "2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 构造执行环境，使用eventTime处理窗口数据
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> unionStream = env.readTextFile(filePaths[0]);

    }
}
