package com.practicekafka.jpa;

import org.springframework.data.repository.CrudRepository;

import com.practicekafka.entity.FailureRecord;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {

}
