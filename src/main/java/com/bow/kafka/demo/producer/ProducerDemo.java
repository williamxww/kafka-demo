package com.bow.kafka.demo.producer;

import java.io.IOException;
import com.bow.kafka.util.MqProducer;

public class ProducerDemo {

    public static void main(String[] args) throws IOException {

        MqProducer producer1 = new MqProducer();
        MqProducer producer2 = new MqProducer();

        boolean closeFlag = false;
        while (true) {
            char close = (char) System.in.read();
            if (close == 'c') {
                producer2.close();
                closeFlag = true;
                System.out.println("close producer2");
            }
            System.out.println("send one message");
            producer1.send("mytopic", "2", "value1");
            if(!closeFlag){
                producer2.send("mytopic", "2", "value2");
            }

        }

    }

}
