package com.begind.sample;

import com.begind.constant.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerQuickStart {

    public static void main(String[] args) {

        //1. kafka的配置信息
        Properties prop = new Properties();
        //kafka的链接信息
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        //配置重试次数
        prop.put(ProducerConfig.RETRIES_CONFIG, 5);
        //数据压缩
        prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"lz4");
        //ack配置  消息确认机制   默认ack=1,即只要集群首领节点收到消息，生产者就会收到一个来自服务器的成功响应
//        prop.put(ProducerConfig.ACKS_CONFIG,"all");

//        消息key的序列化器
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //消息value的序列化器
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //2. 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        //封装发送的消息
        ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(KafkaConstants.INPUT_TOPIC, "key_001", "hello kafka");
        ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(KafkaConstants.INPUT_TOPIC, "key_002", "hello world");
        //3. 发送消息
        producer.send(producerRecord1);
        producer.send(producerRecord2);

        //4. 关闭消息通道  必须关闭，否则消息发不出去
        producer.close();

    }
}
