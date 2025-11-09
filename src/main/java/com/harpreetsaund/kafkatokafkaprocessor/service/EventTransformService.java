package com.harpreetsaund.kafkatokafkaprocessor.service;

import com.harpreetsaund.kafkatokafkaprocessor.mapper.TransactionEventMapper;
import com.harpreetsaund.raw.transaction.avro.RawTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class EventTransformService {

    private final TransactionEventMapper transactionEventMapper;

    public EventTransformService(TransactionEventMapper transactionEventMapper) {
        this.transactionEventMapper = transactionEventMapper;
    }

    public Message<TransactionEvent> transformRawTransactionEvent(Message<RawTransactionEvent> message) {
        RawTransactionEvent rawTransactionEvent = message.getPayload();

        TransactionEvent transactionEvent = transactionEventMapper.toTransactionEvent(rawTransactionEvent);

        return MessageBuilder.withPayload(transactionEvent).copyHeaders(message.getHeaders()) //
                .setHeader(KafkaHeaders.KEY, transactionEvent.getHeaders().getEventId()) //
                .build();
    }

    public Message<String> transformErroredEvent(Message<?> message) {
        return MessageBuilder.withPayload(message.toString()).copyHeaders(message.getHeaders()) //
                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString()) //
                .build();
    }
}
