package com.practicekafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practicekafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {
    @Autowired
    private LibraryEventsService libraryEventsService;

    @KafkaListener(topics = { "${topics.retry}" }, groupId = "retry-listener-group")
    // retry topic defined in application.yml
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord in Retry Consumer : {}", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
