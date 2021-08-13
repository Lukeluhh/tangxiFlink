package com.tangxi.flink.utils;

import com.alibaba.fastjson.JSON;
import com.tangxi.flink.model.Metric;
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
public class KafkaUtils {
    public static final String broker_list = "47.97.249.106:9092";
    public static final String topic = "metric3";  // kafka topic，Flink 程序中需要和这个统一

    public static void writeToKafka() throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        tags.put("cluster", "tangxi");
        tags.put("host_ip", "47.94.249.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        Future send = producer.send(record);
        Object o = send.get();
        System.out.println("发送数据: " + JSON.toJSONString(metric));

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
