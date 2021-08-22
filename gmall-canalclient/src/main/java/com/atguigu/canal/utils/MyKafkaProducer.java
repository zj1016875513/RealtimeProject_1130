package com.atguigu.canal.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by VULCAN on 2021/4/26
 *
 *      提供一个连接kafka的生产者；
 *      提供一个方法，生成数据到kafka
 */
public class MyKafkaProducer {

    private  static Producer<String,String> producer;

    // 只会创建一个生产者，不会重复创建
    static {

        producer=getProducer();

    }

    //提供一个连接kafka的生产者
    public  static Producer<String,String> getProducer(){

        //存放生产者的配置,参考ProducerConfig
        Properties properties = new Properties();

        // 基础配置
        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        return kafkaProducer;

    }

    //提供一个方法，生成数据到kafka
    public  static void  sendMessageToKafka(String topic,String value){
        producer.send(new ProducerRecord<String,String>(topic,value));
    }
}
