package com.harpreetsaund.kafkatokafkaprocessor.service;

import com.harpreetsaund.kafkatokafkaprocessor.mapper.TransactionEventMapper;
import com.harpreetsaund.common.avro.EventEnvelope;
import com.harpreetsaund.transaction.avro.Transaction;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class EventTransformService {

    private final TransactionEventMapper transactionEventMapper;

    public EventTransformService(TransactionEventMapper transactionEventMapper) {
        this.transactionEventMapper = transactionEventMapper;
    }

    public Message<com.harpreetsaund.transaction.avro.EventEnvelope> transformEvent(Message<EventEnvelope> message) {
        EventEnvelope rawEventEnvelope = message.getPayload();

        Transaction transaction = transactionEventMapper.toTransactionEvent(rawEventEnvelope.getPayload());

        com.harpreetsaund.transaction.avro.EventEnvelope transactionEventEnvelope = new com.harpreetsaund.transaction.avro.EventEnvelope();
        transactionEventEnvelope.setEventId(UUID.randomUUID().toString());
        transactionEventEnvelope.setEventType(rawEventEnvelope.getEventType());
        transactionEventEnvelope.setSourceSystem(rawEventEnvelope.getSourceSystem());
        transactionEventEnvelope.setTargetSystem(rawEventEnvelope.getTargetSystem());
        transactionEventEnvelope.setTopicName(rawEventEnvelope.getTopicName());
        transactionEventEnvelope.setTransaction(transaction);
        transactionEventEnvelope.setTimestamp(Instant.now().toEpochMilli());

        return MessageBuilder.withPayload(transactionEventEnvelope).copyHeaders(message.getHeaders()) //
                .setHeader(KafkaHeaders.KEY, transactionEventEnvelope.getEventId()) //
                .build();
    }
}
