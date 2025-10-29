package com.harpreetsaund.kafkatokafkaprocessor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class EnvironmentConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(EnvironmentConfig.class);

    private final Environment environment;

    public EnvironmentConfig(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Environment configuration enabled.");
        logger.info("environment: {}", environment);
    }
}
