package com.bow.kafka.demo.producer;

import java.io.IOException;
import java.util.Properties;

import com.bow.kafka.demo.util.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

	private Producer<String, String> producer;

	public ProducerDemo() {
		Properties properties = null;
		try {
			properties = KafkaConfig.getProperties("kafka/producer.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}
		producer = new KafkaProducer<>(properties);
	}

	public void send(String topic, String key, String value) {
		producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (metadata != null) {
					String msg = String.format("partition=%d, offset=%d, keySize=%s, valueSize=%s",
							metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
							metadata.serializedValueSize());
					LOGGER.info(msg);
				}
				if (exception != null) {
					LOGGER.error("send exception ", exception);
				}
			}
		});
		producer.flush();
	}

	public void close() {
		producer.close();
	}

	public static void main(String[] args) throws IOException {
		ProducerDemo producerDemo = new ProducerDemo();

		while (true) {
			System.in.read();
			System.out.println("send one message");
			producerDemo.send("mytopic", "2", "value");
		}

	}

}
