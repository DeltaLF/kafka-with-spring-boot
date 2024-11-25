package com.practicekafka.jpa;

import org.springframework.data.repository.CrudRepository;

import com.practicekafka.entity.LibraryEvent;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {

}
