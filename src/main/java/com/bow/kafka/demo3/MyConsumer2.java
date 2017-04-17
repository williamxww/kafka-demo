package com.bow.kafka.demo3;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName： Description：
 * <p>
 * company： <br>
 * Copyright：Copyright © 2011 All Rights Reserved<br>
 *
 * @author yuhaipeng
 * @Date 2016/11/4 16:39
 * @since JRE 1.6.0_22 or higher
 */
public class MyConsumer2 implements Runnable {
    private String topic;

    public MyConsumer2(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("接收到: " + message);
        }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "10.9.192.184:2181,10.9.192.184:2182,10.9.192.184:2183");// 声明zk
        properties.put("group.id", "group1");// 必须要使用别的组名称，
                                             // 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        properties.put("consumer.id", "consumer2");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) {
        new Thread(new MyConsumer2("yuhaipeng")).start();// 使用kafka集群中创建好的主题
                                                         // test

    }

}
