package com.star.flink.table;

import com.star.flink.model.Word;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class WordCountTable {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        DataSource<Word> input = env.fromElements(
                new Word("hello", 2),
                new Word("word", 2),
                new Word("hello", 1),
                new Word("test", 1));
        Table table = tableEnv.fromDataSet(input);
        Table select = table.groupBy("word")
                //.select("word,frequency.sum as frequency");
                .select("word,sum(frequency) as frequency").filter("frequency > 2");
        DataSet<Word> dataSet = tableEnv.toDataSet(select, Word.class);
        dataSet.print();
        System.out.println(dataSet.count());
    }

}


