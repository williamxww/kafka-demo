package com.bow.kafka.util;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

/**
 * 根据业务定制Partitioner
 * 
 * @see DefaultPartitioner
 */
public class MqPartitioner implements Partitioner {

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
        int partitionNum = cluster.partitionCountForTopic(topic);
        return key.hashCode() % partitionNum;
    }

}
