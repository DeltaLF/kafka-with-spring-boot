package com.practicekafka.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practicekafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    public String topic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)
            throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        // 1. Blocking call - get metadata about the kafka cluster (only the first time)
        // 2. Send message happens - Returns a completeableFuture
        var completeFuture = kafkaTemplate.send(topic, key, value);

        return completeFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
                });
    }

    public SendResult<Integer, String> sendLibraryEventBlocking(LibraryEvent libraryEvent)
            throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        // with get specified
        // 1. Blocking call - get metadata about the kafka cluster (only the first time)
        // 2. Block and wait until the message is sent to the kafka broker
        var sendResult = kafkaTemplate.send(topic, key, value)
                // .get();
                .get(3, TimeUnit.SECONDS);

        // because it's blocking, there will be InterruptedException, ExecutionException
        // needed to be handled
        handleSuccess(key, value, sendResult);
        return sendResult;

    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventProducerRecord(LibraryEvent libraryEvent)
            throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord = buildProducerRecord(key, value);

        var completeFuture = kafkaTemplate.send(producerRecord);

        return completeFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
                });

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent to the producer: key: {}, value: {}, partition: {}", key, value,
                sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending message to producer and the exception is {}", throwable.getMessage(), throwable);
    }
}
