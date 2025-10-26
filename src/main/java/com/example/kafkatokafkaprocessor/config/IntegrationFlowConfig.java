package com.example.kafkatokafkaprocessor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerProperties;

@Configuration
public class IntegrationFlowConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationFlowConfig.class);

    @Bean
    public IntegrationFlow kafkaToKafkaFlow(ConsumerFactory<String, String> consumerFactory,
            ConsumerProperties consumerProperties) {
        return IntegrationFlow.from(Kafka.inboundChannelAdapter(consumerFactory, consumerProperties))
                .handle(System.out::println).get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Integration flow configuration enabled.");
    }
}
