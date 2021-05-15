package com.star.flink.batch;

import com.star.flink.model.Word;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class WordCountPojo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.readTextFile("E:/workspace/flink_start/data");
        DataSet<Word> count = data.flatMap(new FlatMapFunction<String, Word>() {
            @Override
            public void flatMap(String value, Collector<Word> out) throws Exception {
                out.collect(new Word(value.split(",")[0], 1));
            }
        }).groupBy("word")
                .reduce(new ReduceFunction<Word>() {
            @Override
            public Word reduce(Word value1, Word value2) throws Exception {
                return new Word(value1.getWord(),value1.getFrequency()+value2.getFrequency());
            }
        });
        count.print();
    }
}
