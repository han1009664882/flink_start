package com.star.flink.batch;

import com.star.flink.model.Word;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.readTextFile("E:/workspace/flink_start/data");
        DataSet<Tuple2<String, Integer>> count = data.flatMap(
                //注意：flink不支持java8的lamda
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new Tuple2<>(split[0], 1));
            }
        }).groupBy(0).sum(1);


        //map
        //DataSet<Tuple2<String, Integer>> count = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
        //    @Override
        //    public Tuple2<String, Integer> map(String value) throws Exception {
        //        return new Tuple2<>(value.split(",")[0], 1);
        //    }
        //}).groupBy(0).sum(1);

        count.print();
    }
}
