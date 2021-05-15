package com.star.flink.table;

import com.star.flink.model.Person;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersonTable {
    private static final Logger LOG = LoggerFactory.getLogger(PersonTable.class);

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        DataSource<String> data = env.readTextFile("E:/workspace/flink_start/data");
        DataSet<Person> dataSet = data.map(x -> {
            String[] split = x.split(",");
            return new Person(split[0], split[1], Integer.valueOf(split[2]));
        });

        //法一:
        //tableEnv.registerDataSet("person", dataSet);
        //Table query = tableEnv.sqlQuery("select name,sex,age from person where age >10");

        //法二:
        Table table = tableEnv.fromDataSet(dataSet);
        Table query = table.select("name,sex,age").where("age>10");

        DataSet<Person> result = tableEnv.toDataSet(query, Person.class);

        result.print();
        //LOG.info("----> count:{}",result.count());
    }
}
