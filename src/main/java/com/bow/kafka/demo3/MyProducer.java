package com.bow.kafka.demo3;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * ClassName： Description：
 * <p>
 * company： <br>
 * Copyright：Copyright © 2011 All Rights Reserved<br>
 *
 * @author yuhaipeng
 * @Date 2016/11/4 10:32
 * @since JRE 1.6.0_22 or higher
 */
public class MyProducer implements Runnable {
    private String topic;

    private final Logger log = LoggerFactory.getLogger(MyProducer.class);

    public MyProducer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer producer = createProducer();
        int i = 0;
        while (true) {
            producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "10.9.192.184:2181,10.9.192.184:2182,10.9.192.184:2183");// 声明zk
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "10.9.192.184:10002,10.9.192.184:10003,10.9.192.184:10004");// 声明kafka
                                                                                                           // broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new Thread(new MyProducer("yuhaipeng")).start();// 使用kafka集群中创建好的主题 test

    }

}
