package com.bow.kafka.demo.producer;

import java.io.IOException;

import com.bow.kafka.demo.consumer.ConsumerDemo;
import com.bow.kafka.util.MqProducer;

/**
 * producer 的简单示例
 * @see ConsumerDemo
 */
public class ProducerDemo {

    public static void main(String[] args) throws IOException {

        MqProducer producer1 = new MqProducer();
        System.out.println("ready.");
        while (true) {
            char close = (char) System.in.read();
            System.out.println("send one message");
            producer1.send("mytopic", "2", "value1");
        }

    }

}
