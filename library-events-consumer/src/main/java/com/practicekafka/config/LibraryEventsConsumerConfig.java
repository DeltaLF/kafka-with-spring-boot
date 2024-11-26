package com.practicekafka.config;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practicekafka.service.FailureService;

import lombok.extern.slf4j.Slf4j;;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @Autowired
    private FailureService failureService;

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());

                    }

                });
        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("# exception in consumerRecordRecoverer : {}", e.getMessage());
        var record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            // return new TopicPartition(retryTopic, r.partition());
            // recoverable logic
            failureService.saveFailedRecord(record, e, RETRY);
        } else {
            // return new TopicPartition(deadLetterTopic, r.partition());
            // non recoverable logic
            failureService.saveFailedRecord(record, e, DEAD);

        }
    };

    public DefaultErrorHandler errorHandler() {

        var exceptionsToIgnoreList = List.of(IllegalArgumentException.class, JsonProcessingException.class);
        var exceptionsToRetryList = List.of(RecoverableDataAccessException.class);

        var fixedBackOff = new FixedBackOff(1000L, 2);
        // retry twice with delay 1s
        var errorHandler = new DefaultErrorHandler(
                consumerRecordRecoverer,
                // publishingRecoverer(),
                // aside from passing mesage to try, dlt topics, we can also store it to db
                fixedBackOff);

        // errorHandler.addNotRetryableExceptions();
        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        // errorHandler.addRetryableExceptions();
        exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            ex.printStackTrace();
            log.info("### Failed Record :{} in Retry Listener, Exception : {}, c : {}", record, ex,
                    deliveryAttempt);
        });
        return errorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3); // spawn 3 kafka listeners (recommend for kafka not running in cloud)
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
