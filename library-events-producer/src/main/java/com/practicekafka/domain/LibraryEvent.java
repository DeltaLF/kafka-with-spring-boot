package com.practicekafka.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEvent,
        @NotNull @Valid Book book) {

}
