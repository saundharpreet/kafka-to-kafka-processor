package com.example.kafkatokafkaprocessor.config;

import com.harpreetsaund.common.avro.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.messaging.MessageChannel;

@Configuration
@EnableIntegration
public class IntegrationConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationConfig.class);

    @Value("${inbound-channel.topic}")
    private String inboundTopic;

    @Value("${inbound-channel.group-id}")
    private String groupId;

    @Bean
    public MessageChannel discardChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public MessageChannel outboundKafkaChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public MessageChannel outboundKafkaSuccessChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public MessageChannel outboundKafkaFailureChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public ContainerProperties containerProperties() {
        ContainerProperties containerProperties = new ContainerProperties(inboundTopic);
        containerProperties.setGroupId(groupId);

        return containerProperties;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, EventEnvelope> listenerContainer(
            ConsumerFactory<String, EventEnvelope> consumerFactory, ContainerProperties containerProperties) {
        ConcurrentMessageListenerContainer<String, EventEnvelope> listenerContainer = new ConcurrentMessageListenerContainer<>(
                consumerFactory, containerProperties);
        listenerContainer.setConcurrency(1);

        return listenerContainer;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Integration configuration enabled.");
    }
}
