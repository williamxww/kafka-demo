package com.bow.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {
    private static Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    public static Properties getProperties(String configFile) throws IOException {
        Properties properties;
        try {
            InputStream input = KafkaConfig.class.getClassLoader().getResourceAsStream(configFile);
            properties = new Properties();
            properties.load(input);
            input.close();
        } catch (IOException e) {
            log.error("IOException ", e);
            throw e;
        }
        return properties;
    }

}
