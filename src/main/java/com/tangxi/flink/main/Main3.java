package com.tangxi.flink.main;

import com.tangxi.flink.sink.PrintSinkFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName:Main
 * @Description: TODO
 * @Author: tangxi
 * @Date: 2021/8/12
 **/
public class Main3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "47.97.249.106:9092");
        props.put("zookeeper.connect", "47.97.249.106:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "metric3",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(1);

        dataStreamSource.addSink(new PrintSinkFunction<>());

        env.execute("Flink add data source");
    }
}
