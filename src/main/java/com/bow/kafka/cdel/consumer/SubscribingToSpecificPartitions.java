package com.bow.kafka.cdel.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.bow.kafka.cdel.util.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


/**指定的有哪些分区处理消息
 * @author dell
 *
 */
public class SubscribingToSpecificPartitions {

	public static void main(String[] args) throws IOException {
		Properties properties = KafkaConfig.getProperties("kafka/consumer3.properties");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		// 消息处理
		TopicPartition partition0 = new TopicPartition("topic1", 0);
		TopicPartition partition1 = new TopicPartition("topic1", 1);
		consumer.assign(Arrays.asList(partition0, partition1));
		try {
	         while(true) {
	        	 final int minBatchSize = 3;
	        	 List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
	             ConsumerRecords<String, String> records = consumer.poll(100);
	             for (TopicPartition partition : records.partitions()) {
	                 List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
	                 for (ConsumerRecord<String, String> record : partitionRecords) {
	                	 buffer.add(record);
	                	 System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	                 }
	                 if (buffer.size() >= minBatchSize) {
	                	 insertIntoDb(buffer);
	                	 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
	                	 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
	                	 buffer.clear();
	                 }
	             }
	         }
	     } finally {
	       consumer.close();
	     }
	}

	/**
	 * 连续消费三次后，插入数据库，然后手动提交，这可以防止多次访问数据库，
	 * 而且如果在插入时失败，下次可以从失败的offset处重新处理。
	 * 	offset = 8, key = abc, value = 444414:45:52.555 [main] DEBUG org.apache.kafka.clients.consumer.internals.AbstractCoordinator - Received successful heartbeat response for group memoryTest3
		offset = 9, key = abc, value = 444414:45:58.590 [main] DEBUG org.apache.kafka.clients.consumer.internals.AbstractCoordinator - Received successful heartbeat response for group memoryTest3
		offset = 10, key = abc, value = 44443
		插入数据库成功
	 * @param buffer
	 */
	private static void insertIntoDb(List<ConsumerRecord<String, String>> buffer){
		System.out.println(buffer.size());
		System.out.println("插入数据库成功");
	}

}
