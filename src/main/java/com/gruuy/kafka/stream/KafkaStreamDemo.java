package com.gruuy.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * @author: Gruuy
 * @remark:
 * @date: Create in 15:38 2019/10/23
 */
@SuppressWarnings("all")
public class KafkaStreamDemo {
    public static void main(String[] args){
        Properties props = new Properties();
        //stream应用程序名字
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //kafka集群地址   多个逗号隔开
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //序列化与反序列化库
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        //创建拓扑
        final StreamsBuilder builder=new StreamsBuilder();
        //从gruuy（topic）使用这个拓扑构建器构建流
        KStream<String,String> source=builder.stream("gruuy");
        //把source的单词全部拆分为带有\W+运算符的单词
        KStream<String,String> words=source.flatMapValues(a->{
            //分割成list
            List<String> ts = Arrays.asList(a.split("\\W+"));
            //输出  返回
            System.out.println(ts);
            return ts;
        });

        //检查从此构造器创建的类型
        //这里应该输出  源节点KSTREAM-SOURCE-0000000000和汇聚节点KSTREAM-SINK-0000000001
        //我们通过构造器构造也是一样   指定了源为topic gruuy
        //之后通过把gruuy的信息输出到t1  完成了一个汇聚操作
        final Topology topology = builder.build();
        //输出
        System.out.println(topology.describe());

        //创建流应用
        final KafkaStreams streams=new KafkaStreams(topology,props);
        //计数器
        CountDownLatch latch = new CountDownLatch(1);
        //附加关闭处理程序来捕获control-c
        //Runtime.getRuntime().addShutdownHook表示在jvm销毁之前执行这个线程  优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                //计数器减1（为0await不阻塞）
                latch.countDown();
            }
        });
        try {
            streams.start();
            //等待为0
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
