package com.bow.kafka.demo.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.bow.kafka.util.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 指定的有哪些分区处理消息
 * 
 * @author dell
 *
 */
public class SubscribePartition {

	private static final Logger LOGGER = LoggerFactory.getLogger(SubscribePartition.class);

	public static void main(String[] args) throws IOException {
		Properties properties = KafkaConfig.getProperties("kafka/consumer.properties");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		// 消息处理
		TopicPartition partition0 = new TopicPartition("mytopic", 0);
		TopicPartition partition1 = new TopicPartition("mytopic", 1);
		consumer.assign(Arrays.asList(partition0, partition1));
		while (true) {
			final int minBatchSize = 3;
			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (TopicPartition partition : records.partitions()) {
				System.out.println("partition No. " + partition.partition());

				// 获取每个分区的记录
				List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
				for (ConsumerRecord<String, String> record : partitionRecords) {
					buffer.add(record);
					String msg = String.format("partition=%d, offset=%d, key=%s, value=%s", record.partition(),
							record.offset(), record.key(), record.value());
					LOGGER.info(msg);
				}
				if (buffer.size() >= minBatchSize) {
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
					buffer.clear();
				}
			}
		}
	}

}
