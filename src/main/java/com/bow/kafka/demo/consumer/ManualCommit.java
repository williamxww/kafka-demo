package com.bow.kafka.demo.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.bow.kafka.util.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 设置enable.auto.commit=false,通过commitSync/commitAsync进行同步或异步的确认。
 *
 */
public class ManualCommit {

	public static void main(String[] args) throws IOException {
		Properties properties = KafkaConfig.getProperties("kafka/consumer.properties");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		// 消息处理
		consumer.subscribe(Arrays.asList("mytopic"));
		final int minBatchSize = 3;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			long checkPoint = 0;
			for (ConsumerRecord<String, String> record : records) {
				checkPoint = record.offset();
				buffer.add(record);
				String msg = String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(),
						record.value());
				System.out.println(msg);
			}
			if (buffer.size() >= minBatchSize) {
				// 确认消费
				consumer.commitSync();
				System.out.println("commit, checkPoint: " + checkPoint);
				buffer.clear();
			}
		}
	}

}
