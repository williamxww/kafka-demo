package com.bow.kafka.demo.consumer;


import com.bow.kafka.util.MqConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 同一个主题，不同组的两个消费者各自消费不受影响，同组内的消费者竞争消费一条。<br/>
 * Consumer Groups提供了弹性的消费分担模型。当某个Consumer挂掉或者新的Consumer加入，可以re-balance<br/>
 * 自动提交demo(需要设置enable.auto.commit=true，offset自动提交到zk)
 */
public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        MqConsumer consumer = new MqConsumer();
        ConsumerRecords<String, String> records = consumer.poll(100);
        consumer.subscribe("mytopic");
        for (ConsumerRecord<String, String> record : records) {
            String msg = String.format("partition=%d, offset=%d, key=%s,value=%s", record.partition(), record.offset(),
                    record.key(), record.value());
            LOGGER.info(msg);
        }
    }

}
