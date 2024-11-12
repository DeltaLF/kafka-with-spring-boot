package com.practicekafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practicekafka.domain.LibraryEvent;
import com.practicekafka.producer.LibraryEventsProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

        log.info("libraryEvent : {}", libraryEvent);
        // invoke the kafka producer
        // libraryEventsProducer.sendLibraryEvent(libraryEvent);
        libraryEventsProducer.sendLibraryEventBlocking(libraryEvent);
        // approach is block operation (with get specified) so this log will happens
        // after event send to kafka
        log.info("libary event is sent to kafka");
        // this will print before sendLibraryEvent handle success
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}