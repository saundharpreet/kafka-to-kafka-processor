package com.harpreetsaund.kafkatokafkaprocessor.config;

import com.harpreetsaund.kafkaserializer.serializers.ConsumerRecordStringSerializer;
import com.harpreetsaund.transaction.avro.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Configuration
@EnableKafka
public class KafkaConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Primary
    @Bean(name = "kafkaTemplate")
    public KafkaTemplate<String, TransactionEvent> kafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties()));
    }

    @Bean(name = "dlqKafkaTemplate")
    public KafkaTemplate<String, String> dlqKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> properties = kafkaProperties.buildProducerProperties();
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ConsumerRecordStringSerializer.class);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(properties));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Kafka configuration enabled.");
    }
}
