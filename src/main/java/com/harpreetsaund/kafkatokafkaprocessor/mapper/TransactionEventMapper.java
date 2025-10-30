package com.harpreetsaund.kafkatokafkaprocessor.mapper;

import com.harpreetsaund.raw.transaction.avro.RawTransactionEvent;
import com.harpreetsaund.transaction.avro.EventHeaders;
import com.harpreetsaund.transaction.avro.EventPayload;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
public class TransactionEventMapper {

    @Value("${outbound-channel.topic}")
    private String outboundTopic;

    public TransactionEvent toTransactionEvent(RawTransactionEvent rawTransactionEvent) {
        EventHeaders eventHeaders = new EventHeaders();
        eventHeaders.setEventId(UUID.randomUUID().toString());
        eventHeaders.setEventType("TransactionEvent");
        eventHeaders.setSourceSystem("Kafka");
        eventHeaders.setTargetSystem("Kafka");
        eventHeaders.setTopicName(outboundTopic);
        eventHeaders.setTimestamp(Instant.now().toEpochMilli());

        String rawPayload = rawTransactionEvent.getPayload().getPayload();

        EventPayload eventPayload = new EventPayload();
        eventPayload.setTransactionId(StringUtils.trimToEmpty(rawPayload.substring(0, 10)));
        eventPayload.setAccountId(StringUtils.trimToEmpty(rawPayload.substring(10, 22)));

        String amount = StringUtils.trimToEmpty(rawPayload.substring(22, 32));
        eventPayload.setAmount(new BigDecimal(amount).doubleValue());

        eventPayload.setCurrency(StringUtils.trimToEmpty(rawPayload.substring(32, 35)));

        String timestamp = StringUtils.trimToEmpty(rawPayload.substring(35, 48));
        eventPayload.setTimestamp(Long.parseLong(timestamp));

        return new TransactionEvent(eventHeaders, eventPayload);
    }
}
