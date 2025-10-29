package com.harpreetsaund.kafkatokafkaprocessor.config;

import com.harpreetsaund.kafkatokafkaprocessor.service.EventFilterService;
import com.harpreetsaund.kafkatokafkaprocessor.service.EventTransformService;
import com.harpreetsaund.common.avro.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.messaging.MessageHandler;

@Configuration
public class IntegrationFlowConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationFlowConfig.class);

    @Value("${outbound-channel.topic}")
    private String outboundTopic;

    @Bean
    public IntegrationFlow kafkaToKafkaFlow(ConcurrentMessageListenerContainer<String, EventEnvelope> listenerContainer,
            EventFilterService eventFilterService, EventTransformService eventTransformService,
            KafkaTemplate<String, com.harpreetsaund.transaction.avro.EventEnvelope> kafkaTemplate) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(listenerContainer,
                        KafkaMessageDrivenChannelAdapter.ListenerMode.record))
                .filter(eventFilterService, "filterEvent", spec -> spec.discardChannel("discardChannel"))
                .transform(eventTransformService, "transformEvent") //
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate)
                        .topicExpression(new LiteralExpression(outboundTopic))
                        .sendSuccessChannel("outboundKafkaSuccessChannel")
                        .sendFailureChannel("outboundKafkaFailureChannel")) //
                .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "discardChannel")
    public MessageHandler handleDiscardedMessage() {
        return message -> logger.info("Discarded event: {}", message);
    }

    @Bean
    @ServiceActivator(inputChannel = "outboundKafkaSuccessChannel")
    public MessageHandler handleSuccessMessage() {
        return message -> logger.info("Message sent successfully: {}", message);
    }

    @Bean
    @ServiceActivator(inputChannel = "outboundKafkaFailureChannel")
    public MessageHandler handleFailureMessage() {
        return message -> logger.error("Failed to send message: {}", message);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Integration flow configuration enabled.");
    }
}
