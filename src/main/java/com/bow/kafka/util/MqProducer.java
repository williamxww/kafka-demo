package com.bow.kafka.util;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author vv
 * @since 2017/6/4.
 */
public class MqProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqProducer.class);

    private Producer<String, String> producer;


    public MqProducer() {
        Properties properties = null;
        try {
            properties = KafkaConfig.getProperties("kafka/producer.properties");
        } catch (IOException e) {
            LOGGER.error("Fail to config kafka ", e);
        }
        producer = new KafkaProducer(properties);
    }

    public MqProducer(Properties properties) {
        producer = new KafkaProducer(properties);
    }


    public void send(String topic, String value) {
        this.send(topic, null, value);
    }

    public void send(String topic, String key, String value) {
        this.send(topic, key, value, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (metadata != null) {
                    LOGGER.debug(" topic={}, partition={}, offset={}, key={}, value={}", metadata.topic(),
                            metadata.partition(), metadata.offset(), key, value);
                }
                if (exception != null) {
                    LOGGER.error("send exception ", exception);
                }
            }
        });
    }

    public void send(String topic, String key, String value, Callback callback) {
        producer.send(new ProducerRecord<String, String>(topic, key, value), callback);
        producer.flush();
    }

    public void close() {
        producer.close();
    }

}
