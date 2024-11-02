package com.practicekafka.domain;

public record LibraryEvent(
    Integer libraryEventId,
    LibraryEventType libraryEvent,
    Book book
    ) {

}
