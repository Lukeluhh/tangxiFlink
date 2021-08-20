package com.tangxi.flink.utils;

import com.alibaba.fastjson.JSON;
import com.tangxi.flink.model.Metric;
import com.tangxi.flink.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @ClassName:KafkaUtils
 * @Description: TODO
 * @Author: tangxi
 * @Date: 2021/8/10
 **/
public class KafkaUtils2 {
    public static final String broker_list = "47.97.249.106:9092";
    public static final String topic = "student1";  // kafka topic，Flink 程序中需要和这个统一

    public static void writeToKafka() throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 3; i++) {
            Student student = new Student(i, "tangxi" + i, "password" + i, 18 );
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            Future send = producer.send(record);
            Object o = send.get();
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }

        for (int i = 5; i <= 10; i++) {
            Student student = new Student(i, "tangxi" + i, "password" + i, 20 );
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            Future send = producer.send(record);
            Object o = send.get();
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            writeToKafka();
        } catch (ExecutionException e) {
            System.out.println("eeee");
        }
    }

}
