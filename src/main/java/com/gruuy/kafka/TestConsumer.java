package com.gruuy.kafka;

import kafka.admin.TopicCommand;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: Gruuy
 * @remark:
 * @date: Create in 17:38 2019/10/22
 */
public class TestConsumer {
    /** CG的名字 */
    public static String Consumer_Group="testJava.Consumer";
    public static void main(String[] args){
        Properties properties=new Properties();
        // 配置kafka的IP和端口  集群需要逗号隔开   ,localhost:9093,localhost:9094
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        //指定key和value的序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        //
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,Consumer_Group);
        //kafka消费者实体
        KafkaConsumer<String,Long> kafkaConsumer=new KafkaConsumer<>(properties);
        //指定消费的topic
        kafkaConsumer.subscribe(Collections.singletonList("t2" ));

        while (true){
            ConsumerRecords<String,Long> records=kafkaConsumer.poll(1000);
            for(ConsumerRecord<String,Long> record:records){
                System.out.println("Topic:"+record.topic()+"   offset:"+record.offset()+"  message:"+record.value() );
            }
        }
    }
}
