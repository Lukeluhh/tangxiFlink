package com.tangxi.flink.main;


import com.alibaba.fastjson.JSON;
import com.tangxi.flink.model.Student;
import com.tangxi.flink.sink.PrintSinkFunction;
import com.tangxi.flink.sink.SinkToMySQL;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ClassName:Main4
 * @Description: TODO
 * @Author: tangxi
 * @Date: 2021/8/16
 **/
public class Main5 {
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
                props)).setParallelism(2)
                .map(string -> {
                    Student student1 = JSON.parseObject(string, Student.class);
//                    System.out.println(student1);
                    return student1;
                });

        student.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                Student s1 = new Student();
                s1.id = value.id;
                s1.name = value.name;
                s1.password = value.password;
                s1.age = value.age;
                return s1;
            }
        }).flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student value, Collector<Student> out) throws Exception {
//                        if (value.id % 2 == 0) {
//                            out.collect(value);
////                            System.out.println(JSON.toJSONString(value));
//                        }
                out.collect(value);
            }
        }).filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                if (value.id > 0) {
                    return true;
                }
                return false;
            }
        });
        KeyedStream<Student, Integer> studentIntegerKeyedStream = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        });
        WindowedStream<Student, Integer, TimeWindow> window = studentIntegerKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
//        window.apply(new JoinFunction<>())
//        studentIntegerKeyedStream.sum(0);
//        studentIntegerKeyedStream.print();
//        studentIntegerKeyedStream.sum("age");
//        studentIntegerKeyedStream.print();
//        studentIntegerKeyedStream.min(1);
//        studentIntegerKeyedStream.print();
//        studentIntegerKeyedStream.min("key");
//        studentIntegerKeyedStream.max(1);
//        studentIntegerKeyedStream.max("key");
//        studentIntegerKeyedStream.minBy(1);
//        studentIntegerKeyedStream.minBy("key");
//        studentIntegerKeyedStream.maxBy(1);
//        studentIntegerKeyedStream.maxBy("age");
//        studentIntegerKeyedStream.print();

        SingleOutputStreamOperator<Student> reduce = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        }).reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                Student student1 = new Student();
                student1.name = value1.name + value2.name+"---------------------------------";
                student1.id = (value1.id + value2.id) * 2;
                student1.password = value1.password + value2.password;
                student1.age = (value1.age + value2.age) * 2;
                return student1;
            }
        });
        reduce.print();
//        student.addSink(new SinkToMySQL()); //数据 sink 到 mysql
//        reduce.addSink(new PrintSinkFunction<>()); //数据 sink 到 mysql

        env.execute("Flink add sink");
    }
}
