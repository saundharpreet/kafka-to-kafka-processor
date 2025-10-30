package com.harpreetsaund.kafkatokafkaprocessor.service;

import com.harpreetsaund.raw.transaction.avro.EventHeaders;
import com.harpreetsaund.raw.transaction.avro.EventPayload;
import com.harpreetsaund.raw.transaction.avro.RawTransactionEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class EventFilterService {

    private static final Logger logger = LoggerFactory.getLogger(EventFilterService.class);

    @Value("${inbound-channel.topic}")
    private String inboundTopic;

    public Boolean filterEvent(Message<RawTransactionEvent> message) {
        logger.debug("Filtering message: {}", message);

        RawTransactionEvent rawTransactionEvent = message.getPayload();

        EventHeaders eventHeaders = rawTransactionEvent.getHeaders();
        if (!StringUtils.equalsIgnoreCase("Kafka", eventHeaders.getSourceSystem())) {
            logger.info("Event filtered out: {}", eventHeaders);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase("Kafka", eventHeaders.getTargetSystem())) {
            logger.info("Event filtered out: {}", eventHeaders);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase(inboundTopic, eventHeaders.getTopicName())) {
            logger.info("Event filtered out: {}", eventHeaders);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase("Transaction", eventHeaders.getEventType())) {
            logger.info("Event filtered out: {}", eventHeaders);
            return Boolean.FALSE;
        }

        EventPayload eventPayload = rawTransactionEvent.getPayload();
        if (StringUtils.length(eventPayload.getPayload()) != 48) {
            logger.info("Event filtered out: {}", eventPayload);
            return Boolean.FALSE;
        }

        logger.debug("Event allowed: {}", rawTransactionEvent);
        return Boolean.TRUE;
    }
}
