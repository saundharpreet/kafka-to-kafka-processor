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
            logger.info("Event filtered due to source system mismatch. Expected 'Kafka' but was '{}'. Headers: {}",
                    eventHeaders.getSourceSystem(), eventHeaders);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase("Kafka", eventHeaders.getTargetSystem())) {
            logger.info("Event filtered due to target system mismatch. Expected 'Kafka' but was '{}'. Headers: {}",
                    eventHeaders.getTargetSystem(), eventHeaders);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase(inboundTopic, eventHeaders.getTopicName())) {
            logger.info("Event filtered due to topic mismatch. Expected '{}' but was '{}'. Headers: {}", inboundTopic,
                    eventHeaders.getTopicName(), eventHeaders);
            return Boolean.FALSE;
        }

        if (!StringUtils.equalsIgnoreCase("RawTransactionEvent", eventHeaders.getEventType())) {
            logger.info(
                    "Event filtered due to event type mismatch. Expected 'RawTransactionEvent' but was '{}'. Headers: {}",
                    eventHeaders.getEventType(), eventHeaders);
            return Boolean.FALSE;
        }

        EventPayload eventPayload = rawTransactionEvent.getPayload();
        int payloadLength = StringUtils.length(eventPayload.getPayload());
        if (payloadLength != 48) {
            logger.info("Event filtered due to payload length mismatch. Expected 48 but was {}. Payload: {}",
                    payloadLength, eventPayload);
            return Boolean.FALSE;
        }

        logger.debug("Event allowed: {}", rawTransactionEvent);
        return Boolean.TRUE;
    }
}
