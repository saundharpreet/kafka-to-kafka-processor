package com.harpreetsaund.kafkatokafkaprocessor.service;

import com.harpreetsaund.kafkatokafkaprocessor.mapper.TransactionEventMapper;
import com.harpreetsaund.raw.transaction.avro.RawTransactionEvent;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class EventTransformService {

    private final TransactionEventMapper transactionEventMapper;

    public EventTransformService(TransactionEventMapper transactionEventMapper) {
        this.transactionEventMapper = transactionEventMapper;
    }

    public Message<TransactionEvent> transformEvent(Message<RawTransactionEvent> message) {
        RawTransactionEvent rawTransactionEvent = message.getPayload();

        TransactionEvent transactionEvent = transactionEventMapper.toTransactionEvent(rawTransactionEvent);

        return MessageBuilder.withPayload(transactionEvent).copyHeaders(message.getHeaders()) //
                .setHeader(KafkaHeaders.KEY, transactionEvent.getHeaders().getEventId()) //
                .build();
    }
}
