package com.bow.kafka.demo.consumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.bow.kafka.demo.util.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 同一个主题，不同组的两个消费者各自消费不受影响，同组内的消费者竞争消费一条。<br/>
 * Consumer Groups提供了弹性的消费分担模型。当某个Consumer挂掉或者新的Consumer加入，可以re-balance<br/>
 * 自动提交demo(需要设置enable.auto.commit=true，offset自动提交到zk)
 */
public class ConsumerDemo {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

	public static void main(String[] args) throws IOException {
		Properties properties = KafkaConfig.getProperties("kafka/consumer.properties");
		KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
		// 消息处理
		consumer.subscribe(Arrays.asList("mytopic"));
		// offset 为记录在分区的唯一标识
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				String msg = String.format("partition=%d, offset=%d, key=%s, value=%s", record.partition(),
						record.offset(), record.key(), record.value());
				LOGGER.info(msg);
			}
		}
	}

}
