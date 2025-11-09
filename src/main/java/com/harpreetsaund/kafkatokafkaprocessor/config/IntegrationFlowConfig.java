package com.harpreetsaund.kafkatokafkaprocessor.config;

import com.harpreetsaund.kafkatokafkaprocessor.service.EventFilterService;
import com.harpreetsaund.kafkatokafkaprocessor.service.EventTransformService;
import com.harpreetsaund.raw.transaction.avro.RawTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
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

    @Value("${recovery-channel.topic}")
    private String recoveryTopic;

    @Bean
    public IntegrationFlow kafkaToKafkaFlow(
            ConcurrentMessageListenerContainer<String, RawTransactionEvent> listenerContainer,
            EventFilterService eventFilterService, EventTransformService eventTransformService,
            KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        return IntegrationFlow
                .from(Kafka
                        .messageDrivenChannelAdapter(listenerContainer,
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record)
                        .errorChannel("errorChannel").payloadType(RawTransactionEvent.class))
                .filter(eventFilterService, "filterEvent", spec -> spec.discardChannel("discardChannel"))
                .transform(eventTransformService, "transformRawTransactionEvent")
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate)
                        .topicExpression(new LiteralExpression(outboundTopic))
                        .sendSuccessChannel("outboundKafkaSuccessChannel")
                        .sendFailureChannel("outboundKafkaFailureChannel"))
                .get();
    }

    @Bean
    public IntegrationFlow erroredMessageFlow(KafkaTemplate<String, String> dlqKafkaTemplate,
            EventTransformService eventTransformService) {
        return IntegrationFlow.from("errorChannel") //
                .transform(eventTransformService, "transformErroredEvent")
                .handle(Kafka.outboundChannelAdapter(dlqKafkaTemplate)
                        .topicExpression(new LiteralExpression(recoveryTopic))
                        .sendSuccessChannel("outboundKafkaSuccessChannel")
                        .sendFailureChannel("outboundKafkaFailureChannel"))
                .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "discardChannel")
    public MessageHandler handleDiscardedMessage() {
        return message -> logger.debug("Discarded event: {}", message);
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
