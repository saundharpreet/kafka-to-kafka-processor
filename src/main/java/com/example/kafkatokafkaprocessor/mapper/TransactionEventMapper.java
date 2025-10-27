package com.example.kafkatokafkaprocessor.mapper;

import com.harpreetsaund.transaction.avro.Transaction;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TransactionEventMapper {

    public Transaction toTransactionEvent(String payload) {
        Transaction transaction = new Transaction();

        transaction.setTransactionId(StringUtils.trimToEmpty(payload.substring(0, 10)));
        transaction.setAccountId(StringUtils.trimToEmpty(payload.substring(10, 22)));

        String amount = StringUtils.trimToEmpty(payload.substring(22, 32));
        transaction.setAmount(new BigDecimal(amount).doubleValue());

        transaction.setCurrency(StringUtils.trimToEmpty(payload.substring(32, 35)));

        String timestamp = StringUtils.trimToEmpty(payload.substring(35, 48));
        transaction.setTimestamp(Long.parseLong(timestamp));

        return transaction;
    }
}
