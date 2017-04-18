package com.bow.kafka.demo.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * 定制Partitioner
 */
public class MyPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> map) {
	}

	@Override
	public void close() {
	}

	/**
	 * 指定消息被投递的分区
	 * 
	 * @return 此消息被分配到的分区号
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		int numPartitions = cluster.partitionCountForTopic(topic);
		try {
			int partitionNum = Integer.parseInt((String) key);
			return Math.abs(partitionNum % numPartitions);
		} catch (Exception e) {
			int num = key.hashCode() % numPartitions;
			return num;
		}
	}

}
