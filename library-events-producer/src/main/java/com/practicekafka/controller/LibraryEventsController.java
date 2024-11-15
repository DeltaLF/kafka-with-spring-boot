package com.practicekafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practicekafka.domain.LibraryEvent;
import com.practicekafka.domain.LibraryEventType;
import com.practicekafka.producer.LibraryEventsProducer;

import jakarta.validation.Valid;
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
            @RequestBody @Valid LibraryEvent libraryEvent)
            throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

        log.info("libraryEvent : {}", libraryEvent);
        // invoke the kafka producer
        // #1 approach
        // libraryEventsProducer.sendLibraryEvent(libraryEvent);
        // #2 approach
        // libraryEventsProducer.sendLibraryEventBlocking(libraryEvent);
        // approach is block operation (with get specified) so this log will happens
        // after event send to kafka
        // #3 approach
        libraryEventsProducer.sendLibraryEventProducerRecord(libraryEvent);
        log.info("libary event is sent to kafka");
        // this will print before sendLibraryEvent handle success
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("v1/libraryevent")
    public ResponseEntity<?> updateLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent)
            throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
        log.info("libraryEvent : {}", libraryEvent);
        ResponseEntity<String> badRequest = validateLibraryEvent(libraryEvent);
        if (badRequest != null)
            return badRequest;

        libraryEventsProducer.sendLibraryEventProducerRecord(libraryEvent);
        // this will print before sendLibraryEvent handle success
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryEventId");
        }

        if (!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }
}