package com.harpreetsaund.kafkatokafkaprocessor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@Configuration
@EnableKafka
public class KafkaConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Kafka configuration enabled.");
    }
}
