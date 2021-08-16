package com.tangxi.flink.main;


import com.alibaba.fastjson.JSON;
import com.tangxi.flink.model.Student;
import com.tangxi.flink.sink.SinkToMySQL;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
/**
 * @ClassName:Main4
 * @Description: TODO
 * @Author: tangxi
 * @Date: 2021/8/16
 **/
public class Main4 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "47.97.249.106:9092");
        props.put("zookeeper.connect", "47.97.249.106:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
                "student1",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> {
                    Student student1 = JSON.parseObject(string, Student.class);
                    System.out.println(student1);
                    return student1;
                }); //Fastjson 解析字符串成 student 对象
        student.addSink(new SinkToMySQL()); //数据 sink 到 mysql

        env.execute("Flink add sink");
    }
}
