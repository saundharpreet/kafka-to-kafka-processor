package com.example.kafkatokafkaprocessor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.kafka.listener.ConsumerProperties;

@Configuration
@EnableIntegration
public class IntegrationConfig implements InitializingBean {

  private static final Logger logger = LoggerFactory.getLogger(IntegrationConfig.class);

  @Value("${inbound-channel.topic}")
  private String inboundTopic;

  @Value("${inbound-channel.group-id}")
  private String groupId;

  @Bean
  public ConsumerProperties consumerProperties() {
    ConsumerProperties consumerProperties = new ConsumerProperties(inboundTopic);
    consumerProperties.setGroupId(groupId);

    return consumerProperties;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    logger.info("Integration configuration enabled.");
  }
}
