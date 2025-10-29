package com.harpreetsaund.kafkatokafkaprocessor.service;

import com.harpreetsaund.common.avro.EventEnvelope;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class EventFilterService {

    private static final Logger logger = LoggerFactory.getLogger(EventFilterService.class);

    @Value("${outbound-channel.topic}")
    private String outboundTopic;

    public Boolean filterEvent(Message<EventEnvelope> message) {
        EventEnvelope eventEnvelope = message.getPayload();

        if (!StringUtils.equalsIgnoreCase("Kafka", eventEnvelope.getSourceSystem())) {
            logger.info("Event filtered out: {}", eventEnvelope);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase("Kafka", eventEnvelope.getTargetSystem())) {
            logger.info("Event filtered out: {}", eventEnvelope);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase(outboundTopic, eventEnvelope.getTopicName())) {
            logger.info("Event filtered out: {}", eventEnvelope);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase("Transaction", eventEnvelope.getEventType())) {
            logger.info("Event filtered out: {}", eventEnvelope);
            return Boolean.FALSE;
        }

        if (StringUtils.length(eventEnvelope.getPayload()) != 48) {
            logger.info("Event filtered out: {}", eventEnvelope);
            return Boolean.FALSE;
        }

        logger.debug("Event allowed: {}", eventEnvelope);
        return Boolean.TRUE;
    }
}
