package com.gruuy.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * @author: Gruuy
 * @remark:
 * @date: Create in 17:13 2019/10/22
 */
public class TestProducer {
    public static String topic="gruuy";
    public static void main(String[] args){
        Properties properties=new Properties();
        // 配置kafka的IP和端口  集群需要逗号隔开   ,localhost:9093,localhost:9094
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        //指定key和value的反序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<>(properties);
        try {
            for (int i=0;i<100;i++){
                String msg="Hello,"+new Random().nextInt(1000);
                //key为话题名，value为发送的message
                ProducerRecord<String,String> record=new ProducerRecord<>(topic,msg);
                //两个参数的方法  第二个参数是回调方法
                kafkaProducer.send(record, (a,b)->System.out.println("Send Success! Offset:"+a.offset()+" partition:"+a.partition()));
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace( );
        } finally {
            kafkaProducer.close();
        }
    }
}
