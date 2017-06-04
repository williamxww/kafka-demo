package com.bow.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author vv
 * @since 2017/6/4.
 */
public class MqConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqConsumer.class);


    private KafkaConsumer<String, String> consumer;

    public MqConsumer(String group) {
        Properties properties = null;
        try {
            properties = KafkaConfig.getProperties("kafka/consumer.properties");
            if(group != null){
                properties.put("group.id", group);
            }
        } catch (IOException e) {
            LOGGER.error("Fail to config kafka consumer ", e);
        }
        consumer = new KafkaConsumer(properties);
    }

    public MqConsumer() {
        this(null);
    }


    public void subscribe(String topic) {
        consumer.subscribe(Arrays.asList(topic));
    }

    public void subscribe(List<TopicPartition> topicPartitions) {
        consumer.assign(topicPartitions);
    }

    public ConsumerRecords<String, String> poll(long timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        return records;
    }


}
