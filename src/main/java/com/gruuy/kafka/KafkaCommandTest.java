package com.gruuy.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.admin.TopicCommand;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author: Gruuy
 * @remark:
 * @date: Create in 11:33 2019/10/23
 */
public class KafkaCommandTest {
//    public static void main(String[] args){
//        //zookeeper地址  后面两个应该是等待时间  最后一个不知道是什么boolean
//        ZkUtils zkUtils=ZkUtils.apply("127.0.0.1:2181",30000,30000, JaasUtils.isZkSecurityEnabled());
//        //创建话题   1为zookeeper 2是topic名字  3是partition  4是replica   5与6不知道
//        AdminUtils.createTopic(zkUtils,"t2",1,1,new Properties(), RackAwareMode.Enforced$.MODULE$);
//        zkUtils.close();
//    }

    public static void main(String[] args){
        updateTopic();
    }
    private void createTopic(){
        //创建主题
        String[] options=new String[]{
                "--create",
                "--zookeeper",
                "localhost:2181",
                "--replication-factor",
                "1",
                "--partitions",
                "1",
                "--topic",
                "test"
        };
        TopicCommand.main(options);
    }
    private void createTopic2(){
        //zookeeper地址  后面两个应该是等待时间  最后一个不知道是什么boolean
        ZkUtils zkUtils=ZkUtils.apply("127.0.0.1:2181",30000,30000, JaasUtils.isZkSecurityEnabled());
        //创建话题   1为zookeeper 2是topic名字  3是partition  4是replica   5与6不知道
        AdminUtils.createTopic(zkUtils,"t2",1,1,new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }
    private static void deleteTopic(){
        //删除话题
        ZkUtils zkUtils=ZkUtils.apply("127.0.0.1:2181",30000,30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.deleteTopic(zkUtils,"test");
        zkUtils.close();
    }

    private static void seeAllTopic2(){
        //只看名字
        ZkUtils zkUtils=ZkUtils.apply("127.0.0.1:2181",30000,30000, JaasUtils.isZkSecurityEnabled());
        List<String> topics= JavaConversions.seqAsJavaList(zkUtils.getAllTopics() );
        topics.stream().forEach(a-> System.out.println(a ));
        zkUtils.close();
    }
    private static void seeAllTopic3(){
        //看给话题添加的动态配置
        ZkUtils zkUtils=ZkUtils.apply("127.0.0.1:2181",30000,30000, JaasUtils.isZkSecurityEnabled());
        Map<String,Properties> configs = JavaConversions.mapAsJavaMap(AdminUtils.fetchAllTopicConfigs(zkUtils));
        for (Map.Entry<String,Properties> entry :  configs.entrySet()){
            System.out.println("key="+entry.getKey()+" ;value= "+entry.getValue());
        }
        zkUtils.close();
    }
    private static void seeAllTopic(){
        //直接输出控制台的查看
        String[] options = new String[]{
                "--list",
                "--zookeeper",
                "localhost:2181"
        };
        TopicCommand.main(options);
    }
    private static void updateTopic(){
        ZkUtils zkUtils=ZkUtils.apply("127.0.0.1:2181",30000,30000, JaasUtils.isZkSecurityEnabled());
        Properties properties=AdminUtils.fetchEntityConfig(zkUtils,ConfigType.Topic(),"gruuy");
        properties.put("replication-factor",2);
        properties.entrySet().iterator().forEachRemaining(a->{
            System.out.print(a.getKey() );
            System.out.println(a.getValue() );
        });
        AdminUtils.changeTopicConfig(zkUtils,"gruuy",properties);
        zkUtils.close();
    }
}
